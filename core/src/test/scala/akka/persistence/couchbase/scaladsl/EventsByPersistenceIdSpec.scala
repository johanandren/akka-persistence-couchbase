/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import akka.persistence.DeleteMessagesSuccess
import akka.persistence.couchbase.TestActor
import akka.persistence.query.Offset
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

class EventsByPersistenceIdSpec extends AbstractQuerySpec("EventsByPersistenceIdSpec") {

  "Couchbase query EventsByPersistenceId" must {

    "find existing events" in new Setup {
      override def initialPersistedEvents = 3

      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(2)
          .expectNext(s"$pid-1", s"$pid-2")
          .expectNoMessage(noMsgTimeout)
          .request(2)
          .expectNext(s"$pid-3")
          .expectComplete()
      }
    }

    "skip deleted events" in new Setup {
      override def initialPersistedEvents = 3
      persistentActor ! TestActor.DeleteTo(2)
      probe.expectMsg(DeleteMessagesSuccess(2))
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(2)
          .expectNext(s"$pid-3")
          .expectComplete()
      }
    }

    "find existing events from a sequence number" in new Setup {
      override def initialPersistedEvents = 10

      val src = queries.currentEventsByPersistenceId(pid, 5L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(_.sequenceNr)
          .runWith(TestSink.probe[Any])
          .request(7)
          .expectNext(5, 6, 7, 8, 9, 10)
          .expectComplete()
      }
    }

    "not see any events if the stream starts after current latest event" in new Setup {
      override def initialPersistedEvents = 3
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 5L, Long.MaxValue)
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(5)
          .expectComplete()
      }
    }

    "find existing events up to a sequence number" in new Setup {
      override def initialPersistedEvents = 3
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, 2L)
        src
          .map(_.sequenceNr)
          .runWith(TestSink.probe[Any])
          .request(5)
          .expectNext(1, 2)
          .expectComplete()
      }
    }

    "not see new events after the query was triggered" in new Setup {
      override def initialPersistedEvents = 3

      readingOurOwnWrites {
        val evts = queries
          .currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
          .runWith(Sink.seq)
          .futureValue
        evts should have size (3) // up to evt 3 is visible
      }

      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext(s"$pid-1", s"$pid-2")
        .expectNoMessage(noMsgTimeout)

      // this event that is written after the query was triggered should not be included in the result
      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      // make sure we could observe the new event 4
      readingOurOwnWrites {
        val evts = queries.currentEventsByPersistenceId(pid, 4, 5).runWith(Sink.seq).futureValue
        evts.map(_.event) should ===(List(s"$pid-4"))
      }

      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext(s"$pid-3")
        .expectComplete() // e-4 not seen

    }

    "not see new events after the query was triggered, across page size" in new Setup {
      override def initialPersistedEvents = queries.settings.pageSize + 1

      readingOurOwnWrites {
        val evts = queries
          .currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
          .runWith(Sink.seq)
          .futureValue
        evts should have size (initialPersistedEvents.toLong)
      }

      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      val notTheEntireFirstPage = queries.settings.pageSize - 1L
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(notTheEntireFirstPage)
        .expectNextN((1L to notTheEntireFirstPage).map(n => s"$pid-$n"))
        .expectNoMessage(noMsgTimeout)

      // this event that is written after the query was triggered should not be included in the result
      val seqNrAfterInitial = initialPersistedEvents + 1L
      persistentActor ! s"$pid-$seqNrAfterInitial"
      probe.expectMsg(s"$pid-$seqNrAfterInitial-done")

      // make sure we could observe the new event
      readingOurOwnWrites {
        val evts = queries
          .currentEventsByPersistenceId(pid, initialPersistedEvents + 1L, Long.MaxValue)
          .runWith(Sink.seq)
          .futureValue
        evts.map(_.event) should ===(List(s"$pid-$seqNrAfterInitial"))
      }

      // here comes the weird part, the query stage should fill up one page and not do another query until
      // we reach the end of it, but then that second query should see the new event and fail this test
      // but it doesn't
      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        // last events that was in db when query was triggered
        .expectNextN(((notTheEntireFirstPage + 1L) to initialPersistedEvents.toLong).map(n => s"$pid-$n"))
        .expectComplete() // but not the event that was written after query is not seen

    }

    "only deliver what requested if there is more in the buffer" in new Setup {
      // FIXME what does this actually test? what buffer?
      override def initialPersistedEvents = 1000
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
        val streamProbe = src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(2)
          .expectNext(s"$pid-1", s"$pid-2")
          .expectNoMessage(noMsgTimeout)

        streamProbe
          .expectNoMessage(noMsgTimeout)
          .request(5)
          .expectNext(s"$pid-3", s"$pid-4", s"$pid-5", s"$pid-6", s"$pid-7")
          .expectNoMessage(noMsgTimeout)

        streamProbe
          .request(5)
          .expectNext(s"$pid-8", s"$pid-9", s"$pid-10", s"$pid-11", s"$pid-12")
          .expectNoMessage(noMsgTimeout)
      }
    }

    "stop if there are no events" in new Setup {
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)

      src
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectComplete()
    }

    "produce correct sequence of sequence numbers and offsets" in new Setup {
      override def initialPersistedEvents = 3
      val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
      readingOurOwnWrites {
        src
          .map(x => (x.persistenceId, x.sequenceNr, x.offset))
          .runWith(TestSink.probe[Any])
          .request(4)
          .expectNext((pid, 1, Offset.sequence(1)), (pid, 2, Offset.sequence(2)), (pid, 3, Offset.sequence(3)))
          .expectComplete()
      }
    }

    "find all events spanning multiple documents" in new Setup {
      override def initialPersistedEvents = 0

      // doc 1
      persistentActor ! TestActor.PersistAll((1 to 10).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // doc 2
      persistentActor ! TestActor.PersistAll((11 to 25).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // doc 3
      persistentActor ! TestActor.PersistAll((26 to 31).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // doc 4
      persistentActor ! s"$pid-32"
      probe.expectMsg(s"$pid-32-done")

      awaitAssert(
        {
          val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
          src
            .map(_.event)
            .runWith(TestSink.probe[Any])
            .request(10)
            .expectNextN((1 to 10).map(i => s"$pid-$i"))
            .expectNoMessage(noMsgTimeout)
            .request(10)
            .expectNextN((11 to 20).map(i => s"$pid-$i"))
            .request(100)
            .expectNextN((21 to 32).map(i => s"$pid-$i"))
            .expectComplete()
        },
        readOurOwnWritesTimeout
      )
    }

    "complete when same number of events as page size" in new Setup {
      override def initialPersistedEvents = queries.settings.pageSize
      val pageSize = initialPersistedEvents
      readingOurOwnWrites {
        val src = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue)
        val streamProbe = src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(pageSize + 10L)

        streamProbe.expectNextN(pageSize.toLong) should ===((1 to pageSize).map(n => s"$pid-$n").toList)
        streamProbe.expectComplete()
      }
    }

  }

  "Couchbase live query EventsByPersistenceId" must {

    "find new events" in new Setup {
      override def initialPersistedEvents = 3
      val streamProbe = readingOurOwnWrites {
        val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
        src
          .map(_.event)
          .runWith(TestSink.probe[Any])
          .request(5)
          .expectNext(s"$pid-1", s"$pid-2", s"$pid-3")
      }
      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      streamProbe.expectNext(s"$pid-4")
      streamProbe.cancel()
    }

    "skip deleted events" in new Setup {
      override def initialPersistedEvents = 3
      persistentActor ! TestActor.DeleteTo(2)
      probe.expectMsg(DeleteMessagesSuccess(2))
      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe =
        readingOurOwnWrites {
          src
            .map(_.event)
            .runWith(TestSink.probe[Any])
            .request(2)
            .expectNext(s"$pid-3")
            .expectNoMessage(noMsgTimeout)
        }

      streamProbe.cancel()
    }

    "find new events if the stream starts after current latest event" in new Setup {
      override def initialPersistedEvents = 4

      val src = queries.eventsByPersistenceId(pid, 5L, Long.MaxValue)
      val streamProbe = src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNoMessage(noMsgTimeout)

      persistentActor ! s"$pid-5"
      probe.expectMsg(s"$pid-5-done")
      persistentActor ! s"$pid-6"
      probe.expectMsg(s"$pid-6-done")

      streamProbe.expectNext(5, 6)

      persistentActor ! s"$pid-7"
      probe.expectMsg(s"$pid-7-done")

      streamProbe.expectNext(7)
      streamProbe.cancel()
    }

    "find new events up to a sequence number" in new Setup {
      override def initialPersistedEvents = 3

      val src = queries.eventsByPersistenceId(pid, 0L, 4L)
      val streamProbe = src
        .map(_.sequenceNr)
        .runWith(TestSink.probe[Any])
        .request(5)
        .expectNext(1, 2, 3)

      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      streamProbe
        .expectNext(4)
        .expectComplete()
    }

    "find new events after demand request" in new Setup {
      override def initialPersistedEvents = 3

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext(s"$pid-1", s"$pid-2")
        .expectNoMessage(noMsgTimeout)

      persistentActor ! s"$pid-4"
      probe.expectMsg(s"$pid-4-done")

      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext(s"$pid-3")
        .expectNext(s"$pid-4")

      streamProbe.cancel()
    }

    "only deliver what requested if there is more in the buffer" in new Setup {
      // FIXME what does this actually test? what buffer?
      override def initialPersistedEvents = 1000

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)
      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNext(s"$pid-1", s"$pid-2")
        .expectNoMessage(noMsgTimeout)

      streamProbe
        .expectNoMessage(noMsgTimeout)
        .request(5)
        .expectNext(s"$pid-3", s"$pid-4", s"$pid-5", s"$pid-6", s"$pid-7")
        .expectNoMessage(noMsgTimeout)

      streamProbe
        .request(5)
        .expectNext(s"$pid-8", s"$pid-9", s"$pid-10", s"$pid-11", s"$pid-12")
        .expectNoMessage(noMsgTimeout)

      streamProbe.cancel()
    }

    "not produce anything if there aren't any events" in new Setup {
      override def initialPersistedEvents = 0 // Database init.

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)

      val streamProbe = src
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(10)
        .expectNoMessage(noMsgTimeout)

      streamProbe.cancel()
    }

    "not produce anything until there are existing events" in new Setup {
      override def initialPersistedEvents = 1 // one existing for actor 1

      // just making sure there are other actor events in the journal
      readingOurOwnWrites {
        val events = queries.currentEventsByPersistenceId(pid, 0L, Long.MaxValue).runWith(Sink.seq).futureValue
        events should have size (1)
      }

      val (pid2, persistentActor2) = startPersistentActor(0)

      // start the live query before there are any events for actor 2
      val actor2Probe = queries
        .eventsByPersistenceId(pid2, 0L, Long.MaxValue)
        .map(_.event)
        .runWith(TestSink.probe[Any])
        .request(2)
        .expectNoMessage(noMsgTimeout)

      // and then write some events (to both actors)
      persistentActor2 ! s"$pid2-1"
      probe.expectMsg(s"$pid2-1-done")
      persistentActor ! s"$pid-2"
      probe.expectMsg(s"$pid-2-done")
      persistentActor2 ! s"$pid2-2"
      probe.expectMsg(s"$pid2-2-done")
      persistentActor ! s"$pid-3"
      probe.expectMsg(s"$pid-3-done")

      // and we should see the events only for actor 2
      actor2Probe
        .expectNext(s"$pid2-1")
        .expectNext(s"$pid2-2")
        .expectNoMessage(noMsgTimeout)

      actor2Probe.cancel()
    }

    "find all events across multiple documents" in new Setup {
      override def initialPersistedEvents = 0

      val src = queries.eventsByPersistenceId(pid, 0L, Long.MaxValue)

      // doc 1
      persistentActor ! TestActor.PersistAll((1 to 10).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // doc 2
      persistentActor ! TestActor.PersistAll((11 to 25).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // doc 3
      persistentActor ! TestActor.PersistAll((26 to 31).map(i => s"$pid-$i"))
      probe.expectMsg("PersistAll-done")
      // doc 4 with a single event
      persistentActor ! s"$pid-32"
      probe.expectMsg(s"$pid-32-done")

      val streamProbe =
        readingOurOwnWrites {
          src
            .map(_.event)
            .runWith(TestSink.probe[Any])
            .request(10)
            .expectNextN((1 to 10).map(i => s"$pid-$i"))
            .expectNoMessage(noMsgTimeout)
            .request(10)
            .expectNextN((11 to 20).map(i => s"$pid-$i"))
            .request(100)
            .expectNextN((21 to 32).map(i => s"$pid-$i"))
        }

      streamProbe.cancel()
    }
  }
}
