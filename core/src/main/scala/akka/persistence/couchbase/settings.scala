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
                                                   readTimeout: FiniteDuration,
                                                   warnAboutMissingIndexes: Boolean)

/**
 * INTERNAL API
 */
@InternalApi
object CouchbaseJournalSettings {

  def apply(config: Config): CouchbaseJournalSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("write.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)
    val writeSettings = CouchbaseWriteSettings(
      parallelism = config.getInt("write.parallelism"),
      replicateTo = parseReplicateTo(config.getString("write.replicate-to")),
      persistTo = parsePersistTo(config.getString("write.persist-to")),
      timeout = config.getDuration("write.write-timeout").toMillis.millis
    )
    val readTimeout = config.getDuration("write.read-timeout").toMillis.millis
    val replayPageSize = config.getInt("write.replay-page-size")
    val warnAboutMissingIndexes = config.getBoolean("write.warn-about-missing-indexes")

    CouchbaseJournalSettings(sessionSettings,
                             bucket,
                             writeSettings,
                             replayPageSize,
                             readTimeout,
                             warnAboutMissingIndexes)
  }

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
                                                                 dispatcher: String,
                                                                 warnAboutMissingIndexes: Boolean)
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

    val warnAboutMissingIndexes = config.getBoolean("write.warn-about-missing-indexes")

    CouchbaseReadJournalSettings(sessionSettings,
                                 bucket,
                                 pageSize,
                                 liveQueryInterval,
                                 eventByTagSettings,
                                 dispatcher,
                                 warnAboutMissingIndexes)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] final case class CouchbaseSnapshotSettings(sessionSettings: CouchbaseSessionSettings, bucket: String)

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] object CouchbaseSnapshotSettings {

  def apply(config: Config): CouchbaseSnapshotSettings = {
    val clientConfig = config.getConfig("connection")
    val bucket = config.getString("snapshot.bucket")
    val sessionSettings = CouchbaseSessionSettings(clientConfig)

    CouchbaseSnapshotSettings(sessionSettings, bucket)
  }
}
