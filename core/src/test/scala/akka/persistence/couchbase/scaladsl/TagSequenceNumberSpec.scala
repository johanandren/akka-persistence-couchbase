/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import java.util.UUID

import akka.persistence.couchbase.internal.CouchbaseSchema.{Queries, TaggedMessageForWrite}
import akka.persistence.couchbase.internal._
import akka.persistence.couchbase.{OutOfOrderEventException, TestActor}
import akka.persistence.query.{NoOffset, TimeBasedUUID}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink
import com.couchbase.client.java.query.N1qlParams
import com.couchbase.client.java.query.consistency.ScanConsistency

import scala.concurrent.duration._

class TagSequenceNumberSpec
    extends AbstractQuerySpec("TagSequenceNumberSpec")
    with AsyncCouchbaseSession
    with Queries
    with TagSequenceNumbering {

  // for using TagSequenceNumbering to verify tag seq nrs
  def log = system.log
  protected def queryConsistency = N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
  protected def asyncSession = queries.session
  def bucketName = "akka"
  implicit def executionContext = system.dispatcher

  "events by tag sequence numbering" must {

    "be monotonic and without holes when actor is stopped and restarted" in new Setup {
      val tag1 = "tag-1"
      val tag2 = "tag-2"
      system.log.debug("tag1: {}, tag2: {}", tag1, tag2)

      var messageCounter = 0L
      def messageN() = {
        messageCounter += 1
        messageCounter
      }

      var currentIncarnation = persistentActor
      def writeMessage(tag: String): Unit = {
        val n = messageN()
        currentIncarnation ! s"$n $tag"
        probe.expectMsg(s"$n $tag-done")
      }

      (1 to 5).foreach { restartN =>
        writeMessage(tag1)
        writeMessage(tag2)
        writeMessage(tag1)

        readingOurOwnWrites {
          currentTagSeqNrFromDb(pid, tag1).futureValue should ===(Some(3 * restartN - 1))
        }
        probe.watch(persistentActor)
        persistentActor ! TestActor.Stop
        probe.expectTerminated(persistentActor, 3.seconds)

        currentIncarnation = system.actorOf(TestActor.props(pid))
        writeMessage(tag1)
        readingOurOwnWrites {
          currentTagSeqNrFromDb(pid, tag1).futureValue should ===(Some(3 * restartN))
        }
      }

    }

    "cause query to fail when there are gaps" in {
      val pid = nextPersistenceId()
      val writerUUID = UUID.randomUUID().toString
      val tag = "tacos"
      val generator = UUIDGenerator()
      var tagSeqNr = 0L
      def nextTagSeqNr = {
        tagSeqNr += 1
        tagSeqNr
      }

      // not really important
      val serializedPayload = SerializedMessage.serialize(SerializationExtension(system), "whatever").futureValue

      // let's fake it till we make it
      val firstTenUuids = (1L to 10L).map { seqNr =>
        val uuid = generator.nextUuid()
        val message = new TaggedMessageForWrite(
          seqNr,
          serializedPayload,
          uuid,
          (tag -> nextTagSeqNr) :: Nil
        )
        val document = CouchbaseSchema.atomicWriteAsJsonDoc(pid, writerUUID, message :: Nil, seqNr)
        couchbaseSession.insert(document).futureValue
        uuid
      }

      // make a gap
      nextTagSeqNr
      val lastSeqNr = 11L
      val lastMessage = new TaggedMessageForWrite(
        lastSeqNr,
        serializedPayload,
        generator.nextUuid(),
        (tag -> nextTagSeqNr) :: Nil
      )
      val document = CouchbaseSchema.atomicWriteAsJsonDoc(pid, writerUUID, lastMessage :: Nil, lastSeqNr)
      couchbaseSession.insert(document).futureValue

      // both current and live query should fail because of it
      awaitAssert(
        {
          val failure =
            queries.currentEventsByTag(tag, TimeBasedUUID(firstTenUuids.head)).runWith(Sink.seq).failed.futureValue

          failure shouldBe an[OutOfOrderEventException]
        },
        readOurOwnWritesTimeout
      )

      // now we know all events are visible, lets do that query from the start
      val streamProbe = queries.eventsByTag(tag, NoOffset).runWith(TestSink.probe)
      streamProbe.request(12)
      streamProbe.expectNextN(10)
      val error = streamProbe.expectError() // should fail because of the gap
      error shouldBe an[OutOfOrderEventException]
    }

  }

}
