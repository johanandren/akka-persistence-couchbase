/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.{CouchbaseSessionSettings, CouchbaseWriteSettings}
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.Config

import scala.concurrent.duration._

final case class CouchbaseJournalSettings private (sessionSettings: CouchbaseSessionSettings,
                                                   bucket: String,
                                                   writeSettings: CouchbaseWriteSettings,
                                                   replayPageSize: Int,
                                                   readTimeout: FiniteDuration)

/**
 * INTERNAL API
 */
@InternalApi
object CouchbaseJournalSettings {

  def apply(config: Config): CouchbaseJournalSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)
    val writeSettings = parseWriteSettings(config)
    val readTimeout = config.getDuration("write.read-timeout").toMillis.millis
    val replayPageSize = config.getInt("write.replay-page-size")

    CouchbaseJournalSettings(sessionSettings, bucket, writeSettings, replayPageSize, readTimeout)
  }

  def parseWriteSettings(config: Config): CouchbaseWriteSettings =
    CouchbaseWriteSettings(
      parallelism = config.getInt("write.parallelism"),
      replicateTo = parseReplicateTo(config.getString("write.replicate-to")),
      persistTo = parsePersistTo(config.getString("write.persist-to")),
      timeout = config.getDuration("write.write-timeout").toMillis.millis
    )

  private def parseReplicateTo(value: String): ReplicateTo = value match {
    case "none" => ReplicateTo.NONE
    case "one" => ReplicateTo.ONE
    case "two" => ReplicateTo.TWO
    case "three" => ReplicateTo.THREE
    case unknown => throw new IllegalArgumentException(s"Unknown replicate-to value: $unknown")
  }

  private def parsePersistTo(value: String): PersistTo = value match {
    case "none" => PersistTo.NONE
    case "master" => PersistTo.MASTER
    case "one" => PersistTo.ONE
    case "two" => PersistTo.TWO
    case "three" => PersistTo.THREE
    case "four" => PersistTo.FOUR
    case unknown => throw new IllegalArgumentException(s"Unknown persist-to value: $unknown")
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] final case class CouchbaseReadJournalSettings(sessionSettings: CouchbaseSessionSettings,
                                                                 bucket: String,
                                                                 pageSize: Int,
                                                                 liveQueryInterval: FiniteDuration,
                                                                 eventByTagSettings: EventByTagSettings,
                                                                 dispatcher: String)
final case class EventByTagSettings(eventualConsistencyDelay: FiniteDuration)

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] object CouchbaseReadJournalSettings {

  def apply(config: Config): CouchbaseReadJournalSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)

    val pageSize = config.getInt("read.page-size")

    val eventByTagConfig = config.getConfig("read.events-by-tag")
    val eventByTagSettings = EventByTagSettings(
      eventByTagConfig.getDuration("eventual-consistency-delay").toMillis.millis
    )
    val liveQueryInterval = config.getDuration("read.live-query-interval").toMillis.millis

    val dispatcher = config.getString("read.plugin-dispatcher")

    CouchbaseReadJournalSettings(sessionSettings, bucket, pageSize, liveQueryInterval, eventByTagSettings, dispatcher)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] final case class CouchbaseSnapshotSettings(sessionSettings: CouchbaseSessionSettings,
                                                              bucket: String,
                                                              writeSettings: CouchbaseWriteSettings)

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] object CouchbaseSnapshotSettings {

  def apply(config: Config): CouchbaseSnapshotSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("snapshot.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)

    // pick these up from the write journal config - it doesn't make sense to allow different
    // settings for events and snapshots
    val writeSettings = CouchbaseJournalSettings.parseWriteSettings(config)

    CouchbaseSnapshotSettings(sessionSettings, bucket, writeSettings)
  }
}
