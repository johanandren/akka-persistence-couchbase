/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.math.Ordering.comparatorToOrdering
import scala.util.Random

class UUIDSpec extends WordSpec with Matchers with ScalaFutures {

  override implicit def patienceConfig = PatienceConfig(5.seconds, 50.millis)

  "The time based UUIDs" should {

    "roundtrip a unix timestamp back into itself" in {
      val unixTimestamp = 1543410889L
      val result = UUIDTimestamp.fromUnixTimestamp(unixTimestamp).toUnixTimestamp
      result should ===(unixTimestamp)
    }
    "roundtrip a max unix timestamp back into itself" in {
      val unixTimestamp = UUIDTimestamp.MaxVal.toUnixTimestamp
      val result = UUIDTimestamp.fromUnixTimestamp(unixTimestamp).toUnixTimestamp
      result should ===(unixTimestamp)
    }
    "roundtrip a min unix timestamp back into itself" in {
      val unixTimestamp = UUIDTimestamp.MinVal.toUnixTimestamp
      val result = UUIDTimestamp.fromUnixTimestamp(unixTimestamp).toUnixTimestamp
      result should ===(unixTimestamp)
    }

    "get a mac that isn't broadcast" in {
      val mac = UUIDGenerator.extractMac()
      // 8th bit should be 0 on a unicast interface
      (mac & 0x010000000000L) should ===(0x000000000000L)
    }

    "generate unique ids (concurrently)" in {
      val generator = UUIDGenerator()

      // Note that max passable here depends on System.currentTimeMillis
      // on Mac intel I7 4Ghz I can do ~400 000 before it starts failing
      // with the too-many-in-the-same-interval exception, we are not expecting
      // that kind of throughput with couchbase though
      val uuids = Future.sequence((0 to 5000).map(_ => Future(generator.nextUuid()))).futureValue
      uuids.forall(uuid => uuids.exists(otherUUID => (uuid ne otherUUID) && (uuid == otherUUID))) should ===(false)
    }

    "sort uuids with a min and a max" in {
      val generator = UUIDGenerator()
      val min = TimeBasedUUIDs.MinUUID
      val justABitBiggerThanMin = TimeBasedUUIDs.create(UUIDTimestamp.MinVal, TimeBasedUUIDs.MaxLSB)
      val pastMinLsb = TimeBasedUUIDs.create(UUIDTimestamp.fromUnixTimestamp(1543410889L), TimeBasedUUIDs.MinLSB)
      val pastMaxLsb = TimeBasedUUIDs.create(UUIDTimestamp.fromUnixTimestamp(1543410889L), TimeBasedUUIDs.MaxLSB)
      val now = generator.nextUuid()
      val endOfTime = TimeBasedUUIDs.MaxUUID

      // (already in sorted order)
      val uuids = List(min, justABitBiggerThanMin, pastMinLsb, pastMaxLsb, now, endOfTime)

      val ordering = comparatorToOrdering(TimeBasedUUIDComparator.comparator)
      val sortedWithComparator = uuids.sorted(ordering)
      sortedWithComparator should ===(uuids)
    }

    "generate sortable string representations" in {
      val generator = UUIDGenerator()
      val uuids = (0 to 5000).map(_ => generator.nextUuid())
      val ordering = comparatorToOrdering(TimeBasedUUIDComparator.comparator)
      val sortedWithComparator = uuids.sorted(ordering)
      val sortedWithStringRepr = uuids.sortBy(TimeBasedUUIDSerialization.toSortableString)

      // original order should be sequential and not change with either sorting
      uuids should ===(sortedWithComparator)
      uuids should ===(sortedWithStringRepr)
    }

    "generate sortable string representations from different hosts" in {
      // 50 random macs
      val macAddresses =
        IndexedSeq(
          "a0:fc:07:e7:bf:b4",
          "28:5c:20:c2:b5:2c",
          "f1:a5:1c:dc:cc:8c",
          "36:3f:5e:37:78:49",
          "80:b3:6d:05:fa:a0",
          "67:c2:34:f7:a2:60",
          "19:78:2b:1a:cd:e2",
          "ae:79:81:ae:86:0b",
          "bf:25:28:53:2d:b4",
          "8c:e4:9c:64:ad:d2",
          "e7:06:e1:29:04:e2",
          "d4:5f:10:39:d1:e6",
          "e4:a8:28:b0:fb:7d",
          "c9:6d:d5:fa:95:9a",
          "5c:32:e1:9c:63:48",
          "bf:8b:bf:87:8f:46",
          "44:0d:9a:7e:6a:14",
          "ca:02:d5:a9:42:4f",
          "2c:7a:bb:0f:f1:c8",
          "b3:71:e8:6a:26:cf",
          "b6:80:8c:fa:96:54",
          "25:41:60:a1:41:32",
          "99:cb:f8:ab:e3:b6",
          "fa:18:16:d3:b1:42",
          "1e:35:f6:78:f3:3f",
          "be:a9:15:00:f1:90",
          "76:32:78:48:68:e6",
          "e5:ee:92:d4:07:e1",
          "ce:1b:fa:c6:0f:87",
          "7d:0e:5e:43:32:1c",
          "4e:14:5f:51:d7:dc",
          "19:22:8a:90:79:ec",
          "72:0b:41:79:08:d5",
          "73:74:a4:0b:d2:d5",
          "d7:54:03:18:51:3a",
          "2a:32:30:8a:3f:fd",
          "3f:90:26:5f:db:3e",
          "e4:64:59:e8:31:a6",
          "12:15:10:24:fb:41",
          "2a:0f:54:d3:83:7e",
          "c5:60:60:68:37:d4",
          "7f:97:bd:8d:79:cc",
          "da:05:d7:3a:d9:a3",
          "a0:bd:88:1f:8a:63",
          "8f:0f:8c:c5:6c:a4",
          "3e:e0:78:41:8f:e0",
          "80:00:a6:32:53:2d",
          "fe:26:ad:ad:b8:e2",
          "02:48:12:99:29:9d",
          "fc:f5:f1:2f:f5:59"
        )
      val generators = macAddresses.map { mac =>
        val macAsLong = UUIDGenerator.macAsLong(mac.split(':').map(s => Integer.parseInt(s, 16).toByte))
        val lsb = TimeBasedUUIDs.lsbFromNode(macAsLong, Random.nextInt())
        new UUIDGenerator(lsb)
      }
      var iterator = generators.iterator
      // every 50 will likely (but not always) get the same timestamp, but different lsb part
      val uuids = (0 to 5000).map { _ =>
        if (!iterator.hasNext) iterator = generators.iterator
        iterator.next().nextUuid()
      }
      val ordering = comparatorToOrdering(TimeBasedUUIDComparator.comparator)
      val sortedWithComparator = uuids.sorted(ordering)
      val sortedWithStringRepr = uuids.sortBy(TimeBasedUUIDSerialization.toSortableString)

      // original order should be sequential and not change with either sorting
      // (mapping it to the rendered format again here makes it a bit easier to read when it fails)
      sortedWithComparator.map(TimeBasedUUIDSerialization.toSortableString) should ===(
        sortedWithStringRepr.map(TimeBasedUUIDSerialization.toSortableString)
      )
    }

    "have a string format that keeps all data" in {
      val generator = UUIDGenerator()
      val uuids = (0 to 9000).map(_ => generator.nextUuid())
      val result = uuids
        .map(TimeBasedUUIDSerialization.toSortableString)
        .map(TimeBasedUUIDSerialization.fromSortableString)

      result should ===(uuids)
    }
  }
}
