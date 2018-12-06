/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import java.util.UUID

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.persistence.couchbase.internal.{
  AsyncCouchbaseSession,
  CouchbaseSchema,
  N1qlQueryStage,
  TimeBasedUUIDComparator,
  TimeBasedUUIDSerialization,
  TimeBasedUUIDs,
  UUIDTimestamp
}
import akka.persistence.couchbase.internal.CouchbaseSchema.Fields
import akka.persistence.couchbase.CouchbaseReadJournalSettings
import akka.persistence.query._
import akka.persistence.query.scaladsl._
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.java.query._
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

object CouchbaseReadJournal {

  /**
   * The default identifier for [[CouchbaseReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#getReadJournalFor`.
   *
   * The value is `"couchbase-journal.read"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  final val Identifier = "couchbase-journal.read"
}

/**
 * Scala API `akka.persistence.query.scaladsl.ReadJournal` implementation for Couchbase.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[akka.persistence.couchbase.javadsl.CouchbaseReadJournal]].
 *
 *
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"couchbase-journal.read"`
 * for the default [[CouchbaseReadJournal#Identifier]]. See `reference.conf`.
 */
class CouchbaseReadJournal(eas: ExtendedActorSystem, config: Config, configPath: String)
    extends ReadJournal
    with AsyncCouchbaseSession
    with EventsByPersistenceIdQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByTagQuery
    with CurrentEventsByTagQuery
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CouchbaseSchema.Queries {

  private implicit val system = eas
  private val log = Logging(system, configPath)

  private val serialization: Serialization = SerializationExtension(system)

  /** INTERNAL API */
  @InternalApi private[akka] val settings: CouchbaseReadJournalSettings = {
    // shared config is one level above the journal specific
    val commonPath = configPath.replaceAll("""\.read$""", "")
    val sharedConfig = system.settings.config.getConfig(commonPath)

    CouchbaseReadJournalSettings(sharedConfig)
  }
  protected def bucketName: String = settings.bucket
  private val n1qlQueryStageSettings = N1qlQueryStage.N1qlQuerySettings(settings.liveQueryInterval, settings.pageSize)
  protected implicit val executionContext = system.dispatchers.lookup(settings.dispatcher)

  protected val asyncSession: Future[CouchbaseSession] =
    CouchbaseSessionRegistry(system).sessionFor(settings.sessionSettings, settings.bucket)

  /**
   * Data Access Object for arbitrary queries or updates.
   */
  val session: Future[CouchbaseSession] = asyncSession

  asyncSession.failed.foreach { ex =>
    log.error(ex, "Failed to connect to couchbase")
  }
  if (settings.warnAboutMissingIndexes) {
    implicit val materializer = ActorMaterializer()
    for {
      session <- asyncSession
      indexes <- session.listIndexes().runWith(Sink.seq)
    } {
      materializer.shutdown() // not needed after used
      val indexNames = indexes.map(_.name()).toSet
      Set("tags", "tags-ordering").foreach(
        requiredIndex =>
          if (!indexNames(requiredIndex))
            log.warning(
              "Missing the [{}] index, the events by tag query will not work without it, se plugin documentation for details",
              requiredIndex
          )
      )
    }
  }

  /**
   * `eventsByPersistenceId` is used to retrieve a stream of events for a particular persistenceId.
   *
   * In addition to the `offset` the `EventEnvelope` also provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * `sequenceNr` and `offset` are always the same for an event and they define ordering for events
   * emitted by this query. Causality is guaranteed (`sequenceNr`s of events for a particular
   * `persistenceId` are always ordered in a sequence monotonically increasing by one). Multiple
   * executions of the same bounded stream are guaranteed to emit exactly the same stream of events.
   *
   * `fromSequenceNr` and `toSequenceNr` can be specified to limit the set of returned events.
   * The `fromSequenceNr` and `toSequenceNr` are inclusive.
   *
   * Deleted events are also deleted from the event stream.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by `currentEventsByPersistenceId`.
   */
  override def eventsByPersistenceId(persistenceId: String,
                                     fromSequenceNr: Long,
                                     toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    internalEventsByPersistenceId(live = true, persistenceId, fromSequenceNr, toSequenceNr)

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    internalEventsByPersistenceId(live = false, persistenceId, fromSequenceNr, toSequenceNr)

  private def internalEventsByPersistenceId(live: Boolean,
                                            persistenceId: String,
                                            fromSequenceNr: Long,
                                            toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    sourceWithCouchbaseSession { session =>
      val deletedTo: Future[Long] =
        firstNonDeletedEventFor(persistenceId, session, 10.seconds) // FIXME timeout from config
          .map(_.getOrElse(fromSequenceNr))

      val source = deletedTo.map { startFrom =>
        if (log.isDebugEnabled)
          log.debug(
            "events by persistenceId: live {}, persistenceId: {}, from: {}, actualFrom: {}, to: {}",
            Array(live, persistenceId, fromSequenceNr, startFrom, toSequenceNr)
          )
        Source
          .fromGraph(
            new N1qlQueryStage[Long](
              live,
              n1qlQueryStageSettings,
              eventsByPersistenceIdQuery(persistenceId, startFrom, toSequenceNr, settings.pageSize),
              session.underlying,
              startFrom, { from =>
                if (from <= toSequenceNr)
                  Some(eventsByPersistenceIdQuery(persistenceId, from, toSequenceNr, settings.pageSize))
                else None
              },
              (_, row) => row.value().getLong(Fields.SequenceNr) + 1
            )
          )
          .mapMaterializedValue(_ => NotUsed)
      }

      Source
        .fromFutureSource(source)
        .mapAsync(1) { row: AsyncN1qlQueryRow =>
          CouchbaseSchema
            .deserializeEvent(row.value(), serialization)
            .map(tpr => EventEnvelope(Offset.sequence(tpr.sequenceNr), tpr.persistenceId, tpr.sequenceNr, tpr.payload))
        }
        .mapMaterializedValue(_ => NotUsed)
    }

  /**
   * `eventsByTag` is used for retrieving events that were marked with
   * a given tag, e.g. all events of an Aggregate Root type.
   *
   * To tag events you create an `akka.persistence.journal.EventAdapter` that wraps the events
   * in a `akka.persistence.journal.Tagged` with the given `tags`.
   *
   * You can use `NoOffset` to retrieve all events with a given tag or
   * retrieve a subset of all events by specifying a `TimeBasedUUID` `offset`.
   *
   * The offset of each event is provided in the streamed envelopes returned,
   * which makes it possible to resume the stream at a later point from a given offset.
   *
   * For querying events that happened after a long unix timestamp you can use
   * [[akka.persistence.couchbase.UUIDs.timeBasedUUIDFrom]] to create the offset to use with this method.
   *
   * In addition to the `offset` the envelope also provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * The returned event stream is ordered by the offset (timestamp), which corresponds
   * to the same order as the write journal stored the events, with inaccuracy due to clock skew
   * between different nodes. The same stream elements (in same order) are returned for multiple
   * executions of the query on a best effort basis. The query is using a Couchbase Indexes
   * that is eventually consistent, so different queries may see different
   * events for the latest events, but eventually the result will be ordered by the timestamp based
   * Couchbase `ordering` field. To compensate for the the eventual consistency the query is
   * delayed to not read the latest events, see `couchbase-journal.read.events-by-tag.eventual-consistency-delay`
   * in reference.conf. However, this is only best effort and in case of network partitions
   * or other things that may delay the updates of the Couchbase indexes the events may be
   * delivered in different order (not strictly by their timestamp).
   *
   * Deleted events are NOT deleted from the tagged event stream.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by `currentEventsByTag`.
   *
   * The stream is completed with failure if there is a failure in executing the query in the
   * backend journal.
   */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    internalEventsByTag(live = true, tag, offset)

  /**
   * Same type of query as `eventsByTag` but the event stream
   * is completed immediately when it reaches the end of the "result set"
   * unless it has received as many events as it has requested.
   * In that case it will request one more time before completing the stream.
   *
   * Use `NoOffset` when you want all events from the beginning of time.
   * To acquire an offset from a long unix timestamp to use with this query, you can use
   * [[akka.persistence.couchbase.UUIDs.timeBasedUUIDFrom]].
   *
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    internalEventsByTag(live = false, tag, offset)

  private def internalEventsByTag(live: Boolean, tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    sourceWithCouchbaseSession { session =>
      val initialOrdering: UUID = offset match {
        case NoOffset => TimeBasedUUIDs.MinUUID
        case Sequence(o) =>
          throw new IllegalArgumentException("Couchbase Journal does not support sequence based offsets")
        case TimeBasedUUID(value) => value
      }
      val initialOrderingString = TimeBasedUUIDSerialization.toSortableString(initialOrdering)

      log.debug("events by tag: live {}, tag: {}, offset: {}", live, tag, initialOrderingString)

      def endOffset: String = {
        val uuid =
          if (settings.eventByTagSettings.eventualConsistencyDelay > Duration.Zero) {
            // offset delay ms back in time from now
            TimeBasedUUIDs.create(
              UUIDTimestamp.fromUnixTimestamp(
                System.currentTimeMillis() - (settings.eventByTagSettings.eventualConsistencyDelay.toMillis)
              ),
              TimeBasedUUIDs.MinLSB
            )
          } else TimeBasedUUIDs.MaxUUID

        TimeBasedUUIDSerialization.toSortableString(uuid)
      }

      // note that the result is "unnested" into the message objects + the persistence id, so the normal
      // document structure does not apply here
      val taggedRows: Source[AsyncN1qlQueryRow, NotUsed] =
        Source
          .fromGraph(
            new N1qlQueryStage[String](
              live,
              n1qlQueryStageSettings,
              eventsByTagQuery(tag, initialOrderingString, endOffset, settings.pageSize),
              session.underlying,
              initialOrderingString, { ordering =>
                Some(eventsByTagQuery(tag, ordering, endOffset, settings.pageSize))
              }, { (_, row) =>
                row.value().getString(Fields.Ordering)
              }
            )
          )

      @volatile var lastUUID = TimeBasedUUIDs.MinUUID

      taggedRows.mapAsync(1) { row: AsyncN1qlQueryRow =>
        val value = row.value()
        CouchbaseSchema.deserializeTaggedEvent(value, Long.MaxValue, serialization).map { tpr =>
          if (TimeBasedUUIDComparator.comparator.compare(tpr.offset, lastUUID) < 0)
            throw new RuntimeException(
              s"Saw time based uuids go backward, last previous [$lastUUID], saw [${tpr.offset}]"
            )
          else
            lastUUID = tpr.offset
          EventEnvelope(Offset.timeBasedUUID(tpr.offset), tpr.pr.persistenceId, tpr.pr.sequenceNr, tpr.pr.payload)
        }
      }
    }

  /**
   * Same type of query as `persistenceIds` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is triggered are not included in the event stream.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] = sourceWithCouchbaseSession { session =>
    log.debug("currentPersistenceIds query")
    // FIXME paging & respect page size for this query as well? #108
    session.streamedQuery(persistenceIdsQuery()).map(_.getString(Fields.PersistenceId))
  }

  /**
   * `persistenceIds` is used to retrieve a stream of `persistenceId`s.
   *
   * The stream emits `persistenceId` strings.
   *
   * The stream guarantees that a `persistenceId` is only emitted once and there are no duplicates.
   * Order is not defined. Multiple executions of the same stream (even bounded) may emit different
   * sequence of `persistenceId`s.
   *
   * The stream is not completed when it reaches the end of the currently known `persistenceId`s,
   * but it continues to push new `persistenceId`s when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * known `persistenceId`s is provided by `currentPersistenceIds`.
   *
   * Note the query is inefficient, especially for large numbers of `persistenceId`s, because
   * of limitation of current internal implementation providing no information supporting
   * ordering/offset queries. The query uses Couchbase's `select distinct` capabilities.
   * More importantly the live query has to repeatedly execute the query each `refresh-interval`,
   * because order is not defined and new `persistenceId`s may appear anywhere in the query results.
   */
  override def persistenceIds(): Source[String, NotUsed] = sourceWithCouchbaseSession { session =>
    log.debug("persistenceIds query")
    Source
      .fromGraph(
        new N1qlQueryStage[NotUsed](
          live = true,
          n1qlQueryStageSettings,
          persistenceIdsQuery(),
          session.underlying,
          NotUsed,
          nextQuery = _ => Some(persistenceIdsQuery()),
          (_, _) => NotUsed
        )
      )
      .statefulMapConcat[String] { () =>
        var seenIds = Set.empty[String]

        { (row) =>
          val id = row.value().getString(Fields.PersistenceId)
          if (seenIds.contains(id)) Nil
          else {
            seenIds += id
            id :: Nil
          }
        }
      }

  }
}
