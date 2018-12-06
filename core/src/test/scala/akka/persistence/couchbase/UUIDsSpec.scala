/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import java.util.UUID

import akka.persistence.couchbase.internal.{TimeBasedUUIDs, UUIDTimestamp}
import akka.persistence.query.{NoOffset, TimeBasedUUID}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class UUIDsSpec extends WordSpec with Matchers with ScalaFutures {

  "UUIDs factory" should {
    "return NoOffset for zero timestamp" in {
      UUIDs.timeBasedUUIDFrom(0) should ===(NoOffset)
    }
    "return a TimeBasedUUID for non zero timestamps" in {
      val now = System.currentTimeMillis()
      val result = UUIDs.timeBasedUUIDFrom(now)

      result shouldBe a[TimeBasedUUID]
      val roundTrip = new UUIDTimestamp(result.asInstanceOf[TimeBasedUUID].value.timestamp()).toUnixTimestamp

      roundTrip should ===(now)
    }
  }
}
