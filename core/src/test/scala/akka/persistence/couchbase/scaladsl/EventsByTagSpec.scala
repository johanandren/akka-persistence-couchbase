/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import akka.persistence.couchbase.{TestActor, UUIDs}
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.{EventEnvelope, NoOffset}
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe

import scala.collection.immutable
import scala.concurrent.duration._

class Tagger extends WriteEventAdapter {

  override def toJournal(event: Any): Any = event match {
    case s: String =>
      val regex = """tag-\w+""".r
      val tags = regex.findAllIn(s).toSet
      if (tags.isEmpty) event
      else Tagged(event, tags)
    case _ => event
  }

  override def manifest(event: Any): String = ""
}

class EventsByTagSpec extends AbstractQuerySpec("EventsByTagSpec") {

  // use unique tags for each test to isolate
  var tagCounter = 0
  private def newTag(): String = {
    tagCounter += 1
    "tag-" + Integer.toHexString(tagCounter)
  }

  "liveEventsByTag" must {

    "find new events" in new Setup {
      val (pid1, a1) = startPersistentActor(0)
      val (pid2, a2) = startPersistentActor(0)
      val (pid3, a3) = startPersistentActor(0) // don't use until after query started

      val tag1 = newTag()
      val tag2 = newTag()
      val tag3 = newTag()
      val tag4 = newTag()
      system.log.debug("tag1: {}, tag2: {}, tag3: {}, tag4: {}", tag1, tag2, tag3, tag4)

      a1 ! "1 hello"
      probe.expectMsg(20.seconds, "1 hello-done")
      a1 ! s"2 $tag1 $tag2"
      probe.expectMsg(s"2 $tag1 $tag2-done")
      val msg3 = s"3 $tag3"
      a2 ! msg3
      probe.expectMsg(s"3 $tag3-done")
      a1 ! "4 something else"
      probe.expectMsg(s"4 something else-done")
      a1 ! s"5 $tag1 $tag4"
      probe.expectMsg(s"5 $tag1 $tag4-done")
      a2 ! s"6 $tag1"
      probe.expectMsg(s"6 $tag1-done")

      val tag3Events = queries.eventsByTag(tag3, offset = NoOffset).runWith(TestSink.probe[Any])
      tag3Events.request(2)
      tag3Events.expectNextPF { case e @ EventEnvelope(_, `pid2`, 1L, `msg3`) => e }
      tag3Events.expectNoMessage(noMsgTimeout)

      val msg7 = s"7 $tag3"
      a3 ! msg7
      probe.expectMsg(s"7 $tag3-done")
      val msg8 = s"8 $tag3"
      a3 ! msg8
      probe.expectMsg(s"8 $tag3-done")

      tag3Events.expectNextPF { case e @ EventEnvelope(_, `pid3`, 1L, `msg7`) => e }
      tag3Events.expectNoMessage(noMsgTimeout)
      tag3Events.request(10)
      tag3Events.expectNextPF { case e @ EventEnvelope(_, `pid3`, 2L, `msg8`) => e }
      tag3Events.cancel()
    }

    "find events from offset " in new Setup {

      val (pid1, a1) = startPersistentActor(0)
      val (pid2, a2) = startPersistentActor(0)
      val (pid3, a3) = startPersistentActor(0) // don't use until after query started

      val tag1 = newTag() // green
      val tag2 = newTag() // apple
      val tag3 = newTag() // black
      val tag4 = newTag() // banana
      system.log.debug("tag1: {}, tag2: {}, tag3: {}, tag4: {}", tag1, tag2, tag3, tag4)

      a1 ! "1 hello"
      probe.expectMsg(20.seconds, "1 hello-done")
      val msg2 = s"2 $tag1 $tag2"
      a1 ! msg2
      probe.expectMsg(s"2 $tag1 $tag2-done")
      a2 ! s"3 $tag3"
      probe.expectMsg(s"3 $tag3-done")
      a1 ! "4 something else"
      probe.expectMsg(s"4 something else-done")
      val msg5 = s"5 $tag1 $tag4"
      a1 ! msg5
      probe.expectMsg(s"5 $tag1 $tag4-done")
      val msg6 = s"6 $tag1"
      a2 ! msg6
      probe.expectMsg(s"6 $tag1-done")

      val tag1Probe1 = queries.eventsByTag(tag1, offset = NoOffset).runWith(TestSink.probe[Any])
      tag1Probe1.request(2)
      tag1Probe1.expectNextPF { case e @ EventEnvelope(_, `pid1`, 2L, `msg2`) => e }
      val offs = tag1Probe1.expectNextPF { case e @ EventEnvelope(_, `pid1`, 4L, `msg5`) => e }.offset
      tag1Probe1.cancel()

      // result from offset should be exclusive - see akka.persistence.query.TimeBasedUUID scaladoc
      system.log.info("Starting from offset {}", offs)
      val tag1Probe2 = queries.eventsByTag(tag1, offs).runWith(TestSink.probe[Any])
      tag1Probe2.request(10)
      tag1Probe2.expectNextPF { case e @ EventEnvelope(_, `pid2`, 2L, `msg6`) => e }
      tag1Probe2.expectNoMessage(noMsgTimeout)
      tag1Probe2.cancel()
    }

    "find events from timestamp offset" in new Setup {
      val (pid1, a1) = startPersistentActor(0)
      val (pid2, a2) = startPersistentActor(0)

      val tag = newTag()
      system.log.debug("tag: {}", tag)

      val msg1 = s"1 $tag"
      a1 ! msg1
      probe.expectMsg(s"1 $tag-done")
      // make "sure" we don't get the same unix timestamp as this test counts on being able to discern
      // events based on that
      Thread.sleep(100)
      val msg2 = s"2 $tag"
      a2 ! msg2
      probe.expectMsg(s"2 $tag-done")
      val timestampBetween2And3 = System.currentTimeMillis()
      Thread.sleep(100)
      val msg3 = s"3 $tag"
      a1 ! msg3
      probe.expectMsg(s"3 $tag-done")

      val tagProbe1 = queries.eventsByTag(tag, offset = NoOffset).runWith(TestSink.probe)
      tagProbe1.request(2)
      tagProbe1.expectNextPF { case e @ EventEnvelope(_, `pid1`, 1L, `msg1`) => e }
      val evt2 = tagProbe1.expectNextPF { case e @ EventEnvelope(_, `pid2`, 1L, `msg2`) => e }

      val startOffset = UUIDs.timeBasedUUIDFrom(timestampBetween2And3)
      val offsetProbe = queries.eventsByTag(tag = tag, startOffset).runWith(TestSink.probe)
      offsetProbe.request(10)
      offsetProbe.expectNextPF { case e @ EventEnvelope(_, `pid1`, 2L, `msg3`) => e }
      offsetProbe.expectNoMessage(noMsgTimeout)
      offsetProbe.cancel()
    }

    "find events from UUID offset" in new Setup {
      val (pid1, a1) = startPersistentActor(0)
      val (pid2, a2) = startPersistentActor(0)

      val tag = newTag()
      system.log.debug("tag: {}", tag)

      val msg1 = s"1 $tag"
      a1 ! msg1
      probe.expectMsg(s"1 $tag-done")
      val msg2 = s"2 $tag"
      a2 ! msg2
      probe.expectMsg(s"2 $tag-done")
      val msg3 = s"3 $tag"
      a1 ! msg3
      probe.expectMsg(s"3 $tag-done")

      val tagProbe1 = queries.eventsByTag(tag, offset = NoOffset).runWith(TestSink.probe[Any])

      tagProbe1.request(2)
      tagProbe1.expectNextPF { case e @ EventEnvelope(_, `pid1`, 1L, `msg1`) => e }
      val offs = tagProbe1.expectNextPF { case e @ EventEnvelope(_, `pid2`, 1L, `msg2`) => e }.offset
      tagProbe1.cancel()

      val tagProbe2 = queries.eventsByTag(tag = tag, offs).runWith(TestSink.probe[Any])
      tagProbe2.request(10)
      tagProbe2.expectNextPF { case e @ EventEnvelope(_, `pid1`, 2L, `msg3`) => e }
      tagProbe2.cancel()
    }

    "stream many events" in new Setup {

      val tag = newTag()
      system.log.debug("tag: {}", tag)

      val tagProbe = queries.eventsByTag(tag, NoOffset).runWith(TestSink.probe[Any])

      for (n <- 1 to 100) {
        persistentActor ! s"$n $tag"
        probe.expectMsg(s"$n $tag-done")
      }

      tagProbe.request(200)
      for (n <- 1 to 100) {
        val Expected = s"$n $tag"
        tagProbe.expectNextPF { case e @ EventEnvelope(_, `pid`, _, Expected) => e }
      }
      tagProbe.expectNoMessage(noMsgTimeout)

      for (n <- 101 to 200) {
        persistentActor ! s"$n $tag"
        probe.expectMsg(s"$n $tag-done")
      }

      for (n <- 101 to 200) {
        val Expected = s"$n $tag"
        tagProbe.expectNextPF { case e @ EventEnvelope(_, `pid`, _, Expected) => e }
      }
      tagProbe.expectNoMessage(noMsgTimeout)

      tagProbe.request(10)
      tagProbe.expectNoMessage(noMsgTimeout)
      tagProbe.cancel()
    }
  }

  "currentEventsByTag" must {
    "find existing events" in new Setup {
      val (pid1, a1) = startPersistentActor(0)
      val (pid2, a2) = startPersistentActor(0)

      val tag1 = newTag()
      val tag2 = newTag()
      val tag3 = newTag()
      system.log.debug("tag1: {}, tag2: {}, tag3: {}", tag1, tag2, tag3)

      a1 ! "hello"
      probe.expectMsg(s"hello-done")
      val msg1 = s"1 $tag1 something"
      val msg2 = s"2 $tag3"
      a1 ! TestActor.PersistAll(List(msg1, msg2))
      probe.expectMsg(s"PersistAll-done")
      val msg3 = s"3 $tag1 $tag2"
      a1 ! msg3
      probe.expectMsg(s"$msg3-done")
      val msg4 = s"4 $tag1"
      a2 ! msg4
      probe.expectMsg(s"$msg4-done")

      system.log.info("Writes complete, starting current queries")

      readingOurOwnWrites {
        val Seq(evt1, evt2, evt3) =
          queries.currentEventsByTag(tag1, NoOffset).runWith(Sink.seq).futureValue
        evt1 should matchPattern { case EventEnvelope(_, `pid1`, 2L, `msg1`) => }
        evt2 should matchPattern { case EventEnvelope(_, `pid1`, 4L, `msg3`) => }
        evt3 should matchPattern { case EventEnvelope(_, `pid2`, 1L, `msg4`) => }
      }

      // after the first read we know the events are queryable
      val Seq(tag2evt1) =
        queries.currentEventsByTag(tag2, NoOffset).runWith(Sink.seq).futureValue
      tag2evt1 should matchPattern { case EventEnvelope(_, `pid1`, 4L, `msg3`) => }

      val Seq(tag3evt1) =
        queries.currentEventsByTag(tag3, NoOffset).runWith(Sink.seq).futureValue
      tag3evt1 should matchPattern { case EventEnvelope(_, `pid1`, 3L, `msg2`) => }
    }

    "find existing events with an offset into a batch" in new Setup {

      val tag1 = newTag()
      val tag2 = newTag()
      system.log.debug("tag1: {}, tag2: {}", tag1, tag2)

      persistentActor ! "hello"
      probe.expectMsg(s"hello-done")
      val msg1 = s"1 $tag1"
      val msg3 = s"3 $tag1"
      persistentActor ! TestActor.PersistAll(List(msg1, s"2 $tag2", msg3, s"4 $tag2"))
      probe.expectMsg(s"PersistAll-done")

      system.log.info("Writes complete, starting current queries")

      // no guarantee we can immediately read our own writes
      val offset = readingOurOwnWrites {
        val tag1evt1 =
          queries
            .currentEventsByTag(tag1, NoOffset)
            .runWith(Sink.head)
            .futureValue

        tag1evt1 match {
          case EventEnvelope(offset, `pid`, _, `msg1`) => offset
        }
      }

      // offset into the same batch write
      val tag1fromOffset =
        queries
          .currentEventsByTag(tag1, offset)
          .runWith(Sink.seq)
          .futureValue
      tag1fromOffset should have size (1)
      tag1fromOffset.head should matchPattern { case EventEnvelope(_, `pid`, _, `msg3`) => }
    }

    "find existing events with an offset into multiple batches" in new Setup {
      val (pid1, ref1) = startPersistentActor(0)
      val (pid2, ref2) = startPersistentActor(0)
      val tag1 = newTag()
      val tag2 = newTag()
      system.log.debug("tag1: {}, tag2: {}", tag1, tag2)

      ref1 ! "hello"
      probe.expectMsg("hello-done")
      ref2 ! "hello"
      probe.expectMsg("hello-done")
      val msg1 = s"1 $tag1"
      val msg3 = s"3 $tag1"
      val msg5 = s"5 $tag1"
      val msg7 = s"7 $tag1"
      ref1 ! TestActor.PersistAll(List(msg1, s"2 $tag2", msg3, s"4 $tag2"))
      ref2 ! TestActor.PersistAll(List(msg5, s"6 $tag2", msg7, s"8 $tag2"))
      probe.expectMsg(s"PersistAll-done")
      probe.expectMsg(s"PersistAll-done")

      system.log.info("Writes complete, starting current queries")

      // no guarantee we can immediately read our own writes
      val tag1FirstThree: immutable.Seq[EventEnvelope] =
        readingOurOwnWrites {
          val res = queries
            .currentEventsByTag(tag1, NoOffset)
            .take(3)
            .runWith(Sink.seq)
            .futureValue

          res should have size (3)
          res
        }

      // offset must be inside second batch of either actor
      val tag1Fromoffset =
        queries
          .currentEventsByTag(tag1, tag1FirstThree.last.offset)
          .runWith(Sink.seq)
          .futureValue
      tag1Fromoffset should have size (1)

      val allRead = (tag1FirstThree ++ tag1Fromoffset).map(_.event.toString).toSet
      allRead should ===(Set(msg1, msg3, msg5, msg7))
    }

  }

}
