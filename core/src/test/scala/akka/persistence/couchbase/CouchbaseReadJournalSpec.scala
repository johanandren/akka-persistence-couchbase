/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.ActorSystem
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{EventEnvelope, NoOffset, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.duration._

class CouchbaseReadJournalSpec
    extends TestKit(ActorSystem("CouchbaseReadJournalSpec"))
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with CouchbaseBucketSetup
    with BeforeAndAfterEach {

  override implicit def patienceConfig = PatienceConfig(testKitSettings.DefaultTimeout.duration, 50.millis)

  private val readOurOwnWritesTimeout = 10.seconds

  protected override def afterAll(): Unit = {
    super.afterAll()
    shutdown(system)
  }

  val waitTime = 100.millis
  lazy val queries: CouchbaseReadJournal =
    PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)

  implicit val mat = ActorMaterializer()

  "currentPersistenceIds" must {
    "work" in {

      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref
      val pa1 = system.actorOf(TestActor.props("p1"))
      pa1 ! "p1-evt-1"
      senderProbe.expectMsg("p1-evt-1-done")
      val pa2 = system.actorOf(TestActor.props("p2"))
      pa2 ! "p2-evt-1"
      senderProbe.expectMsg("p2-evt-1-done")

      awaitAssert(
        {
          val probe: TestSubscriber.Probe[String] = queries.currentPersistenceIds().runWith(TestSink.probe)

          probe.requestNext("p1")
          probe.requestNext("p2")
          probe.expectComplete()
        },
        readOurOwnWritesTimeout
      )

    }
  }

  "live persistenceIds" must {
    "show new persistence ids" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref

      val queryProbe: TestSubscriber.Probe[String] =
        queries.persistenceIds().runWith(TestSink.probe)

      queryProbe.request(10)

      val pa3 = system.actorOf(TestActor.props("p3"))
      pa3 ! "p3-evt-1"
      senderProbe.expectMsg("p3-evt-1-done")

      awaitAssert({
        queryProbe.expectNext("p3")
      }, 5.seconds)

      val pa4 = system.actorOf(TestActor.props("p4"))
      pa4 ! "p4-evt-1"
      senderProbe.expectMsg("p4-evt-1-done")

      // we shouldn't see p3 again
      queryProbe.expectNext("p4")
      // also not after p4 (it could come out of order)
      queryProbe.expectNoMessage(waitTime)

    }
  }

  // FIXME make these test independent i.e. don't rely on writes of previous test
  "liveEventsByTag" must {

    "implement standard EventsByTagQuery" in {
      queries.isInstanceOf[EventsByTagQuery] should ===(true)
    }

    "find new events" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref
      val a = system.actorOf(TestActor.props("a"))
      val b = system.actorOf(TestActor.props("b"))
      val d = system.actorOf(TestActor.props("d")) // don't use until after query started

      a ! "hello"
      senderProbe.expectMsg(20.seconds, s"hello-done")
      a ! "a green apple"
      senderProbe.expectMsg(s"a green apple-done")
      b ! "a black car"
      senderProbe.expectMsg(s"a black car-done")
      a ! "something else"
      senderProbe.expectMsg(s"something else-done")
      a ! "a green banana"
      senderProbe.expectMsg(s"a green banana-done")
      b ! "a green leaf"
      senderProbe.expectMsg(s"a green leaf-done")

      val blackSrc = queries.eventsByTag(tag = "black", offset = NoOffset)
      val probe = blackSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF { case e @ EventEnvelope(_, "b", 1L, "a black car") => e }
      probe.expectNoMessage(waitTime)

      d ! "a black dog"
      senderProbe.expectMsg(s"a black dog-done")
      d ! "a black night"
      senderProbe.expectMsg(s"a black night-done")

      probe.expectNextPF { case e @ EventEnvelope(_, "d", 1L, "a black dog") => e }
      probe.expectNoMessage(waitTime)
      probe.request(10)
      probe.expectNextPF { case e @ EventEnvelope(_, "d", 2L, "a black night") => e }
      probe.cancel()
    }

    "find events from offset " in {
      val greenSrc1 = queries.eventsByTag(tag = "green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF { case e @ EventEnvelope(_, "a", 2L, "a green apple") => e }
      val offs = probe1.expectNextPF { case e @ EventEnvelope(_, "a", 4L, "a green banana") => e }.offset
      probe1.cancel()

      // result from offset should be exclusive - see akka.persistence.query.TimeBasedUUID scaladoc
      system.log.info("Starting from offset {}", offs)
      val greenSrc2 = queries.eventsByTag(tag = "green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF { case e @ EventEnvelope(_, "b", 2L, "a green leaf") => e }
      probe2.expectNoMessage(waitTime)
      probe2.cancel()
    }

    "stream many events" in {
      val e = system.actorOf(TestActor.props("e"))

      val src = queries.eventsByTag(tag = "yellow", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      for (n <- 1 to 100)
        e ! s"yellow-$n"

      probe.request(200)
      for (n <- 1 to 100) {
        val Expected = s"yellow-$n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMessage(waitTime)

      for (n <- 101 to 200)
        e ! s"yellow-$n"

      for (n <- 101 to 200) {
        val Expected = s"yellow-$n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMessage(waitTime)

      probe.request(10)
      probe.expectNoMessage(waitTime)
    }
  }

  "currentEventsByTag" must {
    "find existing events" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref
      val a = system.actorOf(TestActor.props("a1"))
      val b = system.actorOf(TestActor.props("b1"))
      a ! "hello"
      senderProbe.expectMsg(20.seconds, s"hello-done")
      a ! TestActor.PersistAll(List("a blue kiwi", "a pink car"))
      senderProbe.expectMsg(s"PersistAll-done")
      a ! "a blue banana"
      senderProbe.expectMsg(s"a blue banana-done")
      b ! "a blue leaf"
      senderProbe.expectMsg(s"a blue leaf-done")

      system.log.info("Writes complete, starting current queries")

      // no guarantee we can immediately read our own writes
      awaitAssert(
        {
          val blueSrc = queries.currentEventsByTag(tag = "blue", offset = NoOffset)
          val probe = blueSrc.runWith(TestSink.probe[Any])
          probe.request(2)
          probe.expectNextPF { case e @ EventEnvelope(_, "a1", 2L, "a blue kiwi") => e }
          probe.expectNextPF { case e @ EventEnvelope(_, "a1", 4L, "a blue banana") => e }

          probe.expectNoMessage(500.millis)
          probe.request(2)
          probe.expectNextPF { case e @ EventEnvelope(_, "b1", 1L, "a blue leaf") => e }
          probe.expectComplete()
        },
        readOurOwnWritesTimeout
      )

      val pinkSrc = queries.currentEventsByTag(tag = "pink", offset = NoOffset)
      val probe2 = pinkSrc.runWith(TestSink.probe[Any])
      probe2.request(5)
      probe2.expectNextPF { case e @ EventEnvelope(_, "a1", 3L, "a pink car") => e }
      probe2.expectComplete()

      val appleSrc = queries.currentEventsByTag(tag = "kiwi", offset = NoOffset)
      val probe3 = appleSrc.runWith(TestSink.probe[Any])
      probe3.request(5)
      probe3.expectNextPF { case e @ EventEnvelope(_, "a1", 2L, "a blue kiwi") => e }
      probe3.expectComplete()
    }

    "find existing events with an offset into a batch" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref
      val z = system.actorOf(TestActor.props("z1"))

      z ! "hello"
      senderProbe.expectMsg(20.seconds, s"hello-done")
      z ! TestActor.PersistAll(List("a red kiwi", "a yellow car", "a red bottle", "a yellow piranha"))
      senderProbe.expectMsg(s"PersistAll-done")

      system.log.info("Writes complete, starting current queries")

      // no guarantee we can immediately read our own writes
      val offset = awaitAssert(
        {
          val firstReds: immutable.Seq[EventEnvelope] =
            queries
              .currentEventsByTag(tag = "red", offset = NoOffset)
              .take(1)
              .runWith(Sink.seq)
              .futureValue

          firstReds should have size (1)
          firstReds.head match {
            case EventEnvelope(offset, "z1", _, "a red kiwi") => offset
          }
        },
        readOurOwnWritesTimeout
      )

      // offset into the same batch write
      val redsFromOffset =
        queries
          .currentEventsByTag(tag = "red", offset = offset)
          .runWith(Sink.seq)
          .futureValue
      redsFromOffset should have size (1)
      redsFromOffset.head should matchPattern { case EventEnvelope(_, "z1", _, "a red bottle") => }
    }

    "find existing events with an offset into multiple batches" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref
      val x = system.actorOf(TestActor.props("x1"))
      val v = system.actorOf(TestActor.props("v1"))

      x ! "hello"
      senderProbe.expectMsg(20.seconds, s"hello-done")
      v ! "hello"
      senderProbe.expectMsg(20.seconds, s"hello-done")
      x ! TestActor.PersistAll(List("a purple kiwi", "a yellow car", "a purple bottle", "a yellow piranha"))
      v ! TestActor.PersistAll(List("a purple balloon", "a yellow bike", "a purple bowl", "a yellow dolphin"))
      senderProbe.expectMsg(s"PersistAll-done")

      system.log.info("Writes complete, starting current queries")

      // no guarantee we can immediately read our own writes
      val firstPurples: immutable.Seq[EventEnvelope] =
        awaitAssert(
          {
            val res = queries
              .currentEventsByTag(tag = "purple", offset = NoOffset)
              .take(3)
              .runWith(Sink.seq)
              .futureValue

            res should have size (3)
            res
          },
          readOurOwnWritesTimeout
        )

      // offset must be inside second batch of either actor
      val purplesFromOffset =
        queries
          .currentEventsByTag(tag = "purple", offset = firstPurples(2).offset)
          .runWith(Sink.seq)
          .futureValue
      purplesFromOffset should have size (1)

      val allRead = (firstPurples ++ purplesFromOffset).map(_.event.toString).toSet
      allRead should ===(Set("a purple kiwi", "a purple balloon", "a purple bottle", "a purple bowl"))
    }
  }
}
