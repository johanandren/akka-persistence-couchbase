/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.query.Select.select
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.{N1qlParams, N1qlQuery}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._

class RequestTimeoutTestSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll with Eventually {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1500.seconds, 150.millis)

  private implicit val system = ActorSystem("RequestTimeoutTestSpec")
  private implicit val materializer = ActorMaterializer()

  private val bucketName = "RequestTimeoutTestSpec"

  val numberOfDocs: Long = 10 * 1000

  private val queryTimeout: Long = 500

  val env = DefaultCouchbaseEnvironment
    .builder()
    .queryTimeout(queryTimeout)
    .build()

  val cluster = CouchbaseCluster.create(env)
  cluster.authenticate("admin", "admin1")

  {
    val manager = cluster.clusterManager()
    if (manager.hasBucket(bucketName)) {
      cluster.clusterManager().removeBucket(bucketName)
    }
    val bucketSettings = new DefaultBucketSettings.Builder()
      .`type`(BucketType.COUCHBASE)
      .name(bucketName)
      .quota(100)
      .build()
    manager.insertBucket(bucketSettings)
  }

  val bucket = cluster.openBucket(bucketName)
  bucket.bucketManager().dropN1qlPrimaryIndex(true)

  val session = CouchbaseSession(cluster.openBucket(bucketName))

  override protected def afterAll(): Unit = {
    session.close().futureValue
    cluster.clusterManager().removeBucket(bucketName)
    cluster.disconnect()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "Couchbase Java SDK" should {

    "insert data" in {
      val obj = JsonObject.create()
      obj.put("intVal", 5)
      obj.put("stringVal", "whoa")

      val source: Source[Long, NotUsed] = Source.fromIterator(() => (1L to numberOfDocs).toIterator)

      val result = source
        .map { i =>
          JsonDocument.create(s"id-$i", obj.put("id", i))
        }
        .mapAsync(10)(session.insert)
        .runWith(Sink.ignore)

      assert(result.futureValue === Done)

      bucket.bucketManager().createN1qlPrimaryIndex(true, false)
    }

    "fail the stream when the query timeout happens" in eventually {
      val queryParams = N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
      val stmt = select("*").from(bucket.name())
      val query = N1qlQuery.simple(
        stmt,
        queryParams
      )

      val result = session
        .streamedQuery(query)
        .map(d => d.getObject(bucketName).getInt("id"))
        .fold(0L)((acc, i) => acc + i)
        .runWith(Sink.last)

      val resultError = result.failed.futureValue

      assert(resultError.isInstanceOf[CouchbaseResponseException], resultError)
      val ex = resultError.asInstanceOf[CouchbaseResponseException]
      assert(ex.getMessage === s"Timeout ${queryTimeout}ms exceeded")
      assert(ex.code === Some(1080))
    }

  }
}
