/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.UUID

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.event.Logging
import akka.persistence.couchbase.internal.CouchbaseSchema.Queries
import akka.persistence.couchbase.internal._
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.{immutable => im}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object CouchbaseJournal {

  case class TaggedPersistentRepr(pr: PersistentRepr, tags: Set[String], offset: UUID)

  private val ExtraSuccessFulUnit: Try[Unit] = Success(())

}

/**
 * INTERNAL API
 */
@InternalApi
class CouchbaseJournal(config: Config, configPath: String)
    extends AsyncWriteJournal
    with AsyncCouchbaseSession
    with Queries {

  import CouchbaseJournal._
  import akka.persistence.couchbase.internal.CouchbaseSchema.Fields

  private val log = Logging(this)
  protected implicit val system = context.system
  protected implicit val executionContext: ExecutionContext = context.dispatcher

  private val serialization: Serialization = SerializationExtension(system)
  private implicit val materializer = ActorMaterializer()(context)
  private val uuidGenerator = UUIDGenerator()

  // Without this the index that gets the latest sequence nr may have not seen the last write of the last version
  // of this persistenceId. This seems overkill.
  private val replayConsistency = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)

  private val settings: CouchbaseJournalSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.write$""", "")
    val sharedConfig = context.system.settings.config.getConfig(commonPath)

    CouchbaseJournalSettings(sharedConfig)
  }
  private val n1qlQueryStageSettings = N1qlQueryStage.N1qlQuerySettings(
    Duration.Zero, // We don't do live queries
    settings.replayPageSize
  )
  def bucketName: String = settings.bucket

  protected val asyncSession: Future[CouchbaseSession] =
    CouchbaseSessionRegistry(system).sessionFor(settings.sessionSettings, settings.bucket)
  asyncSession.failed.foreach { ex =>
    log.error(ex, "Failed to connect to couchbase")
    context.stop(self)
  }

  if (settings.warnAboutMissingIndexes) {
    for {
      session <- asyncSession
      indexes <- session.listIndexes().runWith(Sink.seq)
    } {
      val indexNames = indexes.map(_.name()).toSet
      Set("persistence-ids", "sequence-nrs").foreach { requiredIndex =>
        if (!indexNames(requiredIndex))
          log.error(
            "Missing the [{}] index, the journal will not work without it, se plugin documentation for details",
            requiredIndex
          )
      }
    }
  }

  override def asyncWriteMessages(messages: im.Seq[AtomicWrite]): Future[im.Seq[Try[Unit]]] = {
    log.debug("asyncWriteMessages {}", messages)
    require(messages.nonEmpty)
    val docsToInsert: Future[im.Seq[JsonDocument]] =
      Future.sequence(messages.map(atomicWriteToJsonDoc)).andThen {
        case Failure(ex) => ex.printStackTrace()
        case _ =>
      }

    docsToInsert.flatMap(docs => Future.sequence(docs.map(insertJsonDoc)))
  }

  private def insertJsonDoc(jsonDoc: JsonDocument): Future[Try[Unit]] =
    withCouchbaseSession { session =>
      session
        .insert(jsonDoc, settings.writeSettings)
        .map(json => ExtraSuccessFulUnit)(ExecutionContexts.sameThreadExecutionContext)
        .recover {
          // lift the failure to not fail the whole sequence
          case NonFatal(ex) => Failure(ex)
        }(ExecutionContexts.sameThreadExecutionContext)
    }

  private def atomicWriteToJsonDoc(write: AtomicWrite): Future[JsonDocument] = {
    // FIXME move document layout/serialization to CouchbaseSchema if possible
    val serializedMessages: Future[Seq[JsonObject]] = Future.sequence(write.payload.map {
      persistentRepr =>
        val (event, tags) = persistentRepr.payload match {
          case t: Tagged => (t.payload.asInstanceOf[AnyRef], t.tags)
          case other => (other.asInstanceOf[AnyRef], Set.empty[String])
        }
        SerializedMessage.serialize(serialization, event).map { message =>
          val json = CouchbaseSchema
            .serializedMessageToObject(message)
            .put(Fields.SequenceNr, persistentRepr.sequenceNr)

          if (tags.nonEmpty) {
            // Event UUID is stored in string field that sorts the same as the in-JVM uuid comparator
            // allowing us to query and sort tagged events server side
            val timeBasedUUID = uuidGenerator.nextUuid()
            json.put(Fields.Tags, JsonArray.from(tags.toList.asJava))
            json.put(Fields.Ordering, TimeBasedUUIDSerialization.toSortableString(timeBasedUUID))
          }

          json
        }
    })
    serializedMessages.map { messagesJson =>
      val pid = write.persistenceId

      val insert: JsonObject = JsonObject
        .create()
        .put(Fields.Type, CouchbaseSchema.JournalEntryType)
        .put(Fields.PersistenceId, pid)
        // assumed all msgs have the same writerUuid
        .put(Fields.WriterUuid, write.payload.head.writerUuid.toString)
        .put(Fields.Messages, JsonArray.from(messagesJson.asJava))

      JsonDocument.create(s"$pid-${write.lowestSequenceNr}", insert)
    }(ExecutionContexts.sameThreadExecutionContext)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    withCouchbaseSession { session =>
      log.debug("asyncDeleteMessagesTo {} {}", persistenceId, toSequenceNr)
      val newMetadataEntry: Future[JsonDocument] =
        if (toSequenceNr == Long.MaxValue) {
          log.debug("Journal cleanup (Long.MaxValue)")
          asyncReadHighestSequenceNr(persistenceId, 0).map { highestSeqNr =>
            CouchbaseSchema.metadataEntry(persistenceId, highestSeqNr)
          }
        } else Future.successful(CouchbaseSchema.metadataEntry(persistenceId, toSequenceNr))

      newMetadataEntry
        .flatMap(entry => session.upsert(entry, settings.writeSettings))
        .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
    }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = withCouchbaseSession { session =>
    log.debug("asyncReplayMessages {} {} {} {}", persistenceId, fromSequenceNr, toSequenceNr, max)

    val deletedTo: Future[Long] = firstNonDeletedEventFor(persistenceId, session, settings.readTimeout)
      .map(_.getOrElse(fromSequenceNr))

    val replayFinished: Future[Unit] = deletedTo.flatMap { firstNonDeletedSeqNr =>
      // important to not start at 0 since that would skew the paging
      val startOfFirstPage = math.max(1, firstNonDeletedSeqNr)
      val endOfFirstPage =
        math.min(startOfFirstPage + settings.replayPageSize - 1, toSequenceNr)

      val firstQuery = replayQuery(persistenceId, startOfFirstPage, endOfFirstPage, replayConsistency)

      log.debug("Starting at sequence_nr {}, query: {}", startOfFirstPage, firstQuery)
      val source: Source[AsyncN1qlQueryRow, NotUsed] = Source.fromGraph(
        new N1qlQueryStage[Long](
          false,
          n1qlQueryStageSettings,
          firstQuery,
          session.underlying,
          endOfFirstPage, { endOfPreviousPage =>
            val startOfNextPage = endOfPreviousPage + 1
            if (startOfNextPage > toSequenceNr) {
              None
            } else {
              val endOfNextPage = math.min(startOfNextPage + settings.replayPageSize, toSequenceNr)
              Some(replayQuery(persistenceId, startOfNextPage, endOfNextPage, replayConsistency))
            }
          },
          (endOfPage, row) => row.value().getLong(Fields.SequenceNr)
        )
      )

      val complete = source
        .take(max)
        .mapAsync(1)(row => CouchbaseSchema.deserializeEvent(row.value(), serialization))
        .runForeach { pr =>
          recoveryCallback(pr)
        }
        .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)

      complete.onComplete {
        // For debugging while developing
        case Failure(ex) => log.error(ex, "Replay error for [{}]", persistenceId)
        case _ =>
      }

      complete
    }

    replayFinished
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    withCouchbaseSession { session =>
      log.debug("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)

      val query = highestSequenceNrQuery(persistenceId, fromSequenceNr, replayConsistency)
      log.debug("Executing: {}", query)

      session
        .singleResponseQuery(query)
        .map {
          case Some(jsonObj) =>
            log.debug("sequence nr: {}", jsonObj)
            if (jsonObj.get("max") != null) jsonObj.getLong("max")
            else 0L
          case None => // should never happen
            0L
        }
    }
}
