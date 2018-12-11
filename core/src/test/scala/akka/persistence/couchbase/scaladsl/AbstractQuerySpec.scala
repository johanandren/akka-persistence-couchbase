/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.scaladsl

import akka.actor.{ActorRef, ActorSystem}
import akka.persistence.couchbase.{CouchbaseBucketSetup, TestActor}
import akka.persistence.query.PersistenceQuery
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{TestKit, TestProbe, WithLogCapturing}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

abstract class AbstractQuerySpec(testName: String)
    extends TestKit(
      ActorSystem(
        testName,
        ConfigFactory.parseString("""
            couchbase-journal.read.page-size = 10
            akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
            akka.loglevel=debug
          """).withFallback(ConfigFactory.load())
      )
    )
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with CouchbaseBucketSetup
    with WithLogCapturing {

  var idCounter = 0
  def nextPersistenceId(): String = {
    idCounter += 1
    val id = Integer.toString(idCounter, 24)
    id.toString
  }

  // provides a unique persistence-id per test case and some initial persisted events
  protected trait Setup {
    lazy val probe = TestProbe()
    implicit def sender: ActorRef = probe.ref
    // note must be a def or lazy val or else it doesn't work (init order)
    def initialPersistedEvents: Int = 0
    def startPersistentActor(initialEvents: Int): (String, ActorRef) = {
      val pid = nextPersistenceId()
      system.log.debug("Starting actor with pid {}, and writing {} initial events", pid, initialPersistedEvents)
      val persistentActor = system.actorOf(TestActor.props(pid))
      if (initialEvents > 0) {
        for (i <- 1 to initialEvents) {
          persistentActor ! s"$pid-$i"
          probe.expectMsg(s"$pid-$i-done")
        }
      }
      (pid, persistentActor)
    }
    val (pid, persistentActor) = startPersistentActor(initialPersistedEvents)

    // no guarantee we can immediately read our own writes
    def readingOurOwnWrites[A](f: => A): A =
      awaitAssert(f, readOurOwnWritesTimeout, interval = 250.millis) // no need to bombard the db with retries
  }

  protected val noMsgTimeout = 100.millis
  protected val readOurOwnWritesTimeout = 10.seconds
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(readOurOwnWritesTimeout)
  implicit val materializer: Materializer = ActorMaterializer()

  lazy // #read-journal-access
  val queries: CouchbaseReadJournal =
    PersistenceQuery(system).readJournalFor[CouchbaseReadJournal](CouchbaseReadJournal.Identifier)
  // #read-journal-access

  protected override def afterAll(): Unit = {
    super.afterAll()
    shutdown(system)
  }

}
