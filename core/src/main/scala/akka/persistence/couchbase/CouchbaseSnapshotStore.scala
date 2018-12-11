/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.couchbase.internal.CouchbaseSchema.Fields
import akka.persistence.couchbase.internal.{AsyncCouchbaseSession, CouchbaseSchema, SerializedMessage}
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Sink
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.error.DocumentDoesNotExistException
import com.couchbase.client.java.query.Delete._
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.couchbase.client.java.query.{N1qlQuery, _}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

final class CouchbaseSnapshotStore(cfg: Config, configPath: String) extends SnapshotStore with AsyncCouchbaseSession {

  private val settings: CouchbaseSnapshotSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.snapshot""", "")
    val sharedConfig = context.system.settings.config.getConfig(commonPath)
    CouchbaseSnapshotSettings(sharedConfig)
  }
  private implicit val system: ActorSystem = context.system
  private implicit val ec: ExecutionContext = context.dispatcher
  private val serialization = SerializationExtension(context.system)

  protected val asyncSession: Future[CouchbaseSession] =
    CouchbaseSessionRegistry(system).sessionFor(settings.sessionSettings, settings.bucket)
  asyncSession.failed.foreach { ex =>
    log.error(ex, "Failed to connect to couchbase")
    context.stop(self)
  }
  if (settings.warnAboutMissingIndexes) {
    implicit val materializer = ActorMaterializer()
    for {
      session <- asyncSession
      indexes <- session.listIndexes().runWith(Sink.seq)
    } {
      materializer.shutdown() // not needed after used
      val indexNames = indexes.map(_.name()).toSet
      if (!indexNames("snapshots"))
        log.error(
          "Missing the [{}] index, the snapshot plugin will not work without it, se plugin documentation for details",
          "snapshots"
        )
    }
  }

  private val queryParams = N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    withCouchbaseSession { session =>
      log.debug("loadAsync: {}, criteria: {}", persistenceId, criteria)

      /* Something like:
       * select * from akka where type = "snapshot"
       * and persistence_id = "p-1"
       * and sequence_nr <= 100
       * and sequence_nr >= 0
       * and timestamp <= 1635974897888
       * order by sequence_nr desc
       * limit 1
       * the filter parts are added as needed and not always present
       */
      val filter = snapshotFilter(criteria)
      val query = select("*")
        .from(settings.bucket)
        .where(filter)
        .orderBy(desc(Fields.SequenceNr))
        .limit(1)

      val result: Future[Option[JsonObject]] = session.singleResponseQuery(
        N1qlQuery.parameterized(query, JsonArray.from("snapshot", persistenceId), queryParams)
      )

      result.flatMap {
        case Some(snapshot) =>
          val value = snapshot.getObject(settings.bucket)
          SerializedMessage.fromJsonObject(serialization, value).map { snapshot =>
            Some(
              SelectedSnapshot(
                SnapshotMetadata(persistenceId, value.getLong(Fields.SequenceNr), value.getLong(Fields.Timestamp)),
                snapshot
              )
            )
          }
        case None => Future.successful(None)
      }
    }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = withCouchbaseSession { session =>
    val ser: Future[SerializedMessage] = SerializedMessage.serialize(serialization, snapshot.asInstanceOf[AnyRef])

    ser.map { serializedMessage =>
      val doc = CouchbaseSchema.snapshotAsJsonDoc(serializedMessage, metadata)
      session
        .upsert(doc, settings.writeSettings)
        .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
    }
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = withCouchbaseSession { session =>
    val key = CouchbaseSchema.snapshotIdFor(metadata)
    session
      .remove(key, settings.writeSettings)
      .recover {
        case _: DocumentDoesNotExistException => ()
      }
      .map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = withCouchbaseSession {
    session =>
      val filter = snapshotFilter(criteria)
      val query = N1qlQuery.parameterized(
        deleteFrom(settings.bucket).where(filter),
        JsonArray.from(CouchbaseSchema.SnapshotEntryType, persistenceId),
        queryParams
      )

      session.singleResponseQuery(query).map(_ => ())(ExecutionContexts.sameThreadExecutionContext)
  }

  private def snapshotFilter(criteria: SnapshotSelectionCriteria): Expression = {
    var filter = x(s"${settings.bucket}.${Fields.Type}")
      .eq("$1")
      .and(x(Fields.PersistenceId).eq("$2"))

    if (criteria.maxSequenceNr != Long.MaxValue)
      filter = filter.and(x(Fields.SequenceNr).lte(criteria.maxSequenceNr))

    if (criteria.minSequenceNr != 0)
      filter = filter.and(x(Fields.SequenceNr).gte(criteria.maxSequenceNr))

    if (criteria.maxTimestamp != Long.MaxValue)
      filter = filter.and(x(Fields.Timestamp).lte(criteria.maxTimestamp))

    if (criteria.minTimestamp != 0)
      filter = filter.and(x(Fields.Timestamp).gte(criteria.minTimestamp))

    filter
  }

}
