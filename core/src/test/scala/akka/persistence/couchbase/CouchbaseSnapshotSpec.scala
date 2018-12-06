/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase
import akka.actor.{ActorSystem, PoisonPill}
import akka.persistence.couchbase.TestActor.{GetLastRecoveredEvent, SaveSnapshot}
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe, WithLogCapturing}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.duration._

class CouchbaseSnapshotSpec
    extends TestKit(
      ActorSystem(
        "CouchbaseSnapshotSpec",
        ConfigFactory.parseString("""
            |akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
          """.stripMargin).withFallback(ConfigFactory.load())
      )
    )
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with CouchbaseBucketSetup
    with BeforeAndAfterEach
    with WithLogCapturing {

  protected override def afterAll(): Unit = {
    super.afterAll()
    shutdown(system)
  }

  val waitTime = 100.millis
  implicit val materializer = ActorMaterializer()

  "entity" should {
    "recover" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref

      {
        val pa1 = system.actorOf(TestActor.props("p1"))
        pa1 ! "p1-evt-1"
        senderProbe.expectMsg("p1-evt-1-done")

        senderProbe.watch(pa1)
        pa1 ! PoisonPill
        senderProbe.expectTerminated(pa1)
      }
      {
        val pa1 = system.actorOf(TestActor.props("p1"))

        pa1 ! GetLastRecoveredEvent
        senderProbe.expectMsg("p1-evt-1")
      }
    }
    "recover after snapshot" in {
      val senderProbe = TestProbe()
      implicit val sender = senderProbe.ref

      {
        val pa1 = system.actorOf(TestActor.props("p2"))
        pa1 ! "p2-evt-1"
        senderProbe.expectMsg("p2-evt-1-done")

        pa1 ! SaveSnapshot
        senderProbe.expectMsgType[Long]

        senderProbe.watch(pa1)
        pa1 ! PoisonPill
        senderProbe.expectTerminated(pa1)
      }
      {
        val pa1 = system.actorOf(TestActor.props("p2"))

        pa1 ! GetLastRecoveredEvent
        senderProbe.expectMsg("p2-evt-1")
      }
    }
  }

}
