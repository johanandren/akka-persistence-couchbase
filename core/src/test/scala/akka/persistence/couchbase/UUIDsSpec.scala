/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.UUID

import akka.persistence.couchbase.internal.{TimeBasedUUIDs, UUIDTimestamp}
import akka.persistence.couchbase.internal.TimeBasedUUIDs.create
import akka.persistence.query.{NoOffset, TimeBasedUUID}
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class UUIDsSpec extends WordSpec with Matchers with ScalaFutures {

  "UUIDs factory" should {
    "return NoOffset for zero timestamp" in {
      assert(UUIDs.timeBasedUUIDFrom(0) === NoOffset)
    }
    "roundtrip current time UUID back into itself" in {
      val uuidTs = UUIDTimestamp.now()
      val uuid: UUID = TimeBasedUUIDs.create(uuidTs, TimeBasedUUIDs.MinLSB)
      val timeBasedUuid: TimeBasedUUID = TimeBasedUUID(uuid)
      val ts: Long = UUIDs.timestampFrom(timeBasedUuid)

      assert(UUIDs.timeBasedUUIDFrom(ts).asInstanceOf[TimeBasedUUID] === timeBasedUuid)
    }
    "roundtrip max possible UUID back into itself" in {
      val maxUuid: UUID = create(UUIDTimestamp.MaxVal, TimeBasedUUIDs.MinLSB)
      val timeBasedUuid: TimeBasedUUID = TimeBasedUUID(maxUuid)

      val ts: Long = UUIDs.timestampFrom(timeBasedUuid)

      assert(UUIDs.timeBasedUUIDFrom(ts).asInstanceOf[TimeBasedUUID] === timeBasedUuid)
    }
  }
}
