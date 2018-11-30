/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal

import java.util.Base64

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.PersistentRepr
import akka.persistence.couchbase.CouchbaseJournal.TaggedPersistentRepr
import akka.persistence.couchbase._
import akka.serialization.{AsyncSerializer, Serialization, Serializers}
import akka.util.OptionVal
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlParams, N1qlQuery, ParameterizedN1qlQuery, SimpleN1qlQuery}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/*
 * INTERNAL API
 */
@InternalApi
private[akka] final object CouchbaseSchema {

  object Fields {
    val Type = "type"

    // journal fields
    /*
       Sample doc structure:
       {
         "persistence_id": "p2",
         "type": "journal_message",
         "writer_uuid": "d61a4212-518a-4f75-8f27-2150f56ae60f",
         // one or more events, more than one in the case of an atomic batched write
         "messages": [
           {
             "sequence_nr": 1,
             "ser_id": 1,
             "ser_manifest": "",
             // either of these two depending on serializer
             "payload_bin": "rO0ABXQACHAyLWV2dC0x",
             "payload": { json- }
             // these two fielsds are only present if there are any tags
             "tags": [ "tag1", "tag2" ]
             // the format of the data is the time based UUID represented as (time in utc)
             // [YYYY]-[MM]-[DD]T[HH]:[mm]:[ss]:[nanoOfSecond]_[lsb-unsigned-long-as-space-padded-string]
             // see akka.persistence.couchbase.internal.TimeBasedUUIDSerialization
             // which makes it possible to sort as a string field and get the same order as sorting the actual time based UUIDs
             "ordering": "1582-10-16T18:52:02.434002368_ 7093823767347982046",
           }
         ],

       }
     */

    // === per doc ===
    val PersistenceId = "persistence_id"
    val WriterUuid = "writer_uuid"
    val Messages = "messages"

    // === per message/event ===
    val SequenceNr = "sequence_nr"
    // A field to sort in a globally consistent way on, for events by tags, based on the UUID
    val Ordering = "ordering"
    // separate fields for json and base64 bin payloads
    val JsonPayload = "payload"
    val BinaryPayload = "payload_bin"
    // the specific tags on an individual message
    val Tags = "tags"

    // metadata object fields
    /*
       Sample doc structure:
       {
         "type": "journal_metadata",
         "deleted_to": 123
       }

     */
    val DeletedTo = "deleted_to"

    /*
      Snapshot doc sample:
      {
        "sequence_nr": 15,
        "ser_manifest": "",
        // either of these two depending on serializer
        "payload_bin": "rO0ABXQACHAyLWV2dC0x",
        "payload": { json- }
        "ser_id": 1,
        "type": "snapshot",
        "timestamp": 1542205413616,
        "persistence_id": "p-1"
      }
     */

    val SerializerManifest = "ser_manifest"
    val SerializerId = "ser_id"
    val Timestamp = "timestamp"

    // === per persistence-id-entry metadata ===
    val MaxSequenceNr = "max_sequence_nr"
  }

  val MetadataEntryType = "journal_metadata"
  val JournalEntryType = "journal_message"
  val SnapshotEntryType = "snapshot"

  trait Queries {
    def bucketName: String

    // TODO how horrific is this query, it does hit the index but it still needs to look at all results?
    // seems to be at worst as fast as the previous ORDER BY + LIMIT 1 query at least
    private lazy val highestSequenceNrStatement =
      s"""
         |SELECT MAX(m.sequence_nr) AS max FROM ${bucketName} a UNNEST messages AS m
         |WHERE a.type = "${CouchbaseSchema.JournalEntryType}"
         |AND a.persistence_id = $$pid
         |AND m.sequence_nr >= $$from
      """.stripMargin

    protected def highestSequenceNrQuery(persistenceId: String, fromSequenceNr: Long, params: N1qlParams): N1qlQuery =
      N1qlQuery.parameterized(
        highestSequenceNrStatement,
        JsonObject
          .create()
          .put("pid", persistenceId)
          .put("from", fromSequenceNr: java.lang.Long),
        params
      )

    private lazy val replayStatement =
      s"""
         |SELECT a.persistence_id, a.writer_uuid, m.* FROM ${bucketName} a UNNEST messages AS m
         |WHERE a.type = "${CouchbaseSchema.JournalEntryType}"
         |AND a.persistence_id = $$pid
         |AND m.sequence_nr >= $$from
         |AND m.sequence_nr <= $$to
         |ORDER BY m.sequence_nr
    """.stripMargin

    protected def replayQuery(persistenceId: String, from: Long, to: Long, params: N1qlParams): N1qlQuery =
      N1qlQuery.parameterized(replayStatement,
                              JsonObject
                                .create()
                                .put("pid", persistenceId)
                                .put("from", from)
                                .put("to", to),
                              params)

    /* Both these queries flattens the doc.messages (which can contain batched writes)
     * into elements in the result and adds a field for the persistence id from
     * the surrounding document. Note that the UNNEST name (m) must match the name used
     * for the array value in the index or the index will not be used for these
     */
    private lazy val eventsByTagQuery =
      s"""
         |SELECT a.persistence_id, m.* FROM ${bucketName} a UNNEST messages AS m
         |WHERE a.type = "${CouchbaseSchema.JournalEntryType}"
         |AND ARRAY_CONTAINS(m.tags, $$tag) = true
         |AND m.ordering > $$fromOffset AND m.ordering <= $$toOffset
         |ORDER BY m.ordering
         |limit $$limit
    """.stripMargin

    protected def eventsByTagQuery(tag: String, fromOffset: String, toOffset: String, pageSize: Int): N1qlQuery =
      N1qlQuery.parameterized(
        eventsByTagQuery,
        JsonObject
          .create()
          .put("tag", tag)
          .put("ordering", fromOffset)
          .put("limit", pageSize)
          .put("fromOffset", fromOffset)
          .put("toOffset", toOffset)
      )

    private lazy val eventsByPersistenceId =
      s"""
         |SELECT a.persistence_id, m.* from ${bucketName} a UNNEST messages AS m
         |WHERE a.type = "${CouchbaseSchema.JournalEntryType}"
         |AND a.persistence_id = $$pid
         |AND m.sequence_nr  >= $$from
         |AND m.sequence_nr <= $$to
         |ORDER by m.sequence_nr
         |LIMIT $$limit
    """.stripMargin

    protected def eventsByPersistenceIdQuery(persistenceId: String,
                                             fromSequenceNr: Long,
                                             toSequenceNr: Long,
                                             pageSize: Int): N1qlQuery =
      N1qlQuery.parameterized(eventsByPersistenceId,
                              JsonObject
                                .create()
                                .put("pid", persistenceId)
                                .put("from", fromSequenceNr)
                                .put("to", toSequenceNr)
                                .put("limit", pageSize))

    // IS NOT NULL is needed to hit the index
    private lazy val persistenceIds =
      s"""
         |SELECT DISTINCT(persistence_id) FROM ${bucketName}
         |WHERE type = "${CouchbaseSchema.JournalEntryType}"
         |AND persistence_id IS NOT NULL
     """.stripMargin

    protected def persistenceIdsQuery(): N1qlQuery =
      N1qlQuery.simple(persistenceIds)

  }

  def metadataIdFor(persistenceId: String): String = s"$persistenceId-meta"

  def metadataEntry(persistenceId: String, deletedTo: Long): JsonDocument =
    JsonDocument.create(
      metadataIdFor(persistenceId),
      JsonObject
        .create()
        .put(Fields.Type, CouchbaseSchema.JournalEntryType)
        .put(Fields.DeletedTo, deletedTo)
    )

  def serializedMessageToObject(msg: SerializedMessage): JsonObject = {
    val json = JsonObject
      .create()
      .put(Fields.SerializerManifest, msg.manifest)
      .put(Fields.SerializerId, msg.identifier)

    msg.nativePayload match {
      case OptionVal.None =>
        json.put(Fields.BinaryPayload, Base64.getEncoder.encodeToString(msg.payload))

      case OptionVal.Some(jsonPayload) =>
        json.put(Fields.JsonPayload, jsonPayload)
    }

    json
  }

  def deserializeEvent[T](
      json: JsonObject,
      serialization: Serialization
  )(implicit system: ActorSystem, ec: ExecutionContext): Future[PersistentRepr] = {
    val persistenceId = json.getString(Fields.PersistenceId)
    val sequenceNr: Long = json.getLong(Fields.SequenceNr)
    val writerUuid = json.getString(Fields.WriterUuid)
    SerializedMessage
      .fromJsonObject(serialization, json)
      .map { payload =>
        PersistentRepr(payload = payload,
                       sequenceNr = sequenceNr,
                       persistenceId = persistenceId,
                       writerUuid = writerUuid)
      }(ExecutionContexts.sameThreadExecutionContext)
  }

  def deserializeTaggedEvent(
      value: JsonObject,
      toSequenceNr: Long,
      serialization: Serialization
  )(implicit ec: ExecutionContext, system: ActorSystem): Future[TaggedPersistentRepr] = {
    val persistenceId = value.getString(Fields.PersistenceId)
    val writerUuid = value.getString(Fields.WriterUuid)
    val sequenceNr = value.getLong(Fields.SequenceNr)
    val tags: Set[String] = value.getArray(Fields.Tags).asScala.map(_.toString).toSet
    SerializedMessage.fromJsonObject(serialization, value).map { payload =>
      CouchbaseJournal.TaggedPersistentRepr(
        PersistentRepr(payload = payload,
                       sequenceNr = sequenceNr,
                       persistenceId = persistenceId,
                       writerUuid = writerUuid),
        tags,
        TimeBasedUUIDSerialization.fromSortableString(value.getString(Fields.Ordering))
      )
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class SerializedMessage(identifier: Int,
                                                 manifest: String,
                                                 payload: Array[Byte],
                                                 nativePayload: OptionVal[JsonObject])

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object SerializedMessage {

  import CouchbaseSchema.Fields

  def serialize(serialization: Serialization,
                event: AnyRef)(implicit system: ActorSystem): Future[SerializedMessage] = {
    val serializer = serialization.findSerializerFor(event)
    val serManifest = Serializers.manifestFor(serializer, event)

    val serId: Int = serializer.identifier

    serializer match {
      case async: AsyncSerializer =>
        Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
          async
            .toBinaryAsync(event)
            .map(bytes => SerializedMessage(serId, serManifest, bytes, OptionVal.None))(
              ExecutionContexts.sameThreadExecutionContext
            )
        }

      case jsonSerializer: JsonSerializer =>
        Future.fromTry(
          Try(SerializedMessage(serId, serManifest, Array.emptyByteArray, OptionVal.Some(jsonSerializer.toJson(event))))
        )

      case _ =>
        Future.fromTry(Try(SerializedMessage(serId, serManifest, serialization.serialize(event).get, OptionVal.None)))
    }
  }

  def fromJsonObject(serialization: Serialization,
                     jsonObject: JsonObject)(implicit system: ActorSystem): Future[Any] = {
    val serId = jsonObject.getInt(Fields.SerializerId)
    val manifest = jsonObject.getString(Fields.SerializerManifest)

    def decodeBytes = {
      val payload = jsonObject.getString(Fields.BinaryPayload)
      Base64.getDecoder.decode(payload)
    }
    val serializer = serialization.serializerByIdentity(serId)

    serializer match {
      case async: AsyncSerializer =>
        Serialization.withTransportInformation(system.asInstanceOf[ExtendedActorSystem]) { () =>
          async.fromBinaryAsync(decodeBytes, manifest)
        }

      case jsonSerializer: JsonSerializer =>
        Future.fromTry(Try(jsonSerializer.fromJson(jsonObject.getObject(Fields.JsonPayload), manifest)))

      case _ =>
        Future.fromTry(serialization.deserialize(decodeBytes, serId, manifest))
    }
  }

}
