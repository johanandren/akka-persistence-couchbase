/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase
import akka.persistence.couchbase.internal.{TimeBasedUUIDs, UUIDGenerator, UUIDTimestamp}
import akka.persistence.query.{NoOffset, Offset, TimeBasedUUID}

object UUIDs {

  /**
   * Create a time based UUID that can be used as offset in `eventsByTag`
   * queries. The `timestamp` is a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   *
   * Note: The host and clock part will be minimum
   */
  def timeBasedUUIDFrom(timestamp: Long): Offset =
    if (timestamp == 0L) NoOffset
    else TimeBasedUUID(TimeBasedUUIDs.create(UUIDTimestamp.fromUnixTimestamp(timestamp), TimeBasedUUIDs.MinLSB))

  /**
   * Convert a `TimeBasedUUID` to a unix timestamp (as returned by
   * `System#currentTimeMillis`.
   *
   * Note that this conversion is lossy
   * since the UUID timestamp is in 100s of nanos. This means a roundtrip
   * through `timeBasedUUIDFrom(timestampFrom(uuid))` can return a different UUID
   * than was passed in.
   */
  def timestampFrom(offset: TimeBasedUUID): Long =
    new UUIDTimestamp(offset.value.timestamp()).toUnixTimestamp

}
