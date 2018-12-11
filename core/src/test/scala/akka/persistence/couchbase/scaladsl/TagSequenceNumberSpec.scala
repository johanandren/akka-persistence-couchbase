/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.couchbase.scaladsl
import akka.persistence.couchbase.TestActor
import akka.persistence.couchbase.internal.CouchbaseSchema.Queries
import akka.persistence.couchbase.internal.{AsyncCouchbaseSession, TagSequenceNumbering}
import com.couchbase.client.java.query.N1qlParams
import com.couchbase.client.java.query.consistency.ScanConsistency

import scala.concurrent.duration._

class TagSequenceNumberSpec
    extends AbstractQuerySpec("TagSequenceNumberSpec")
    with AsyncCouchbaseSession
    with Queries
    with TagSequenceNumbering {

  // for using TagSequenceNumbering to verify tag seq nrs
  def log = system.log
  protected def queryConsistency = N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)
  protected def asyncSession = queries.session
  def bucketName = "akka"
  implicit def executionContext = system.dispatcher

  "events by tag sequence numbering" must {

    "be monotonic and without holes when actor is stopped and restarted" in new Setup {
      val tag1 = "tag-1"
      val tag2 = "tag-2"
      system.log.debug("tag1: {}, tag2: {}", tag1, tag2)

      persistentActor ! s"1 $tag1"
      probe.expectMsg(s"1 $tag1-done")
      persistentActor ! s"2 $tag2"
      probe.expectMsg(s"2 $tag2-done")
      persistentActor ! s"3 $tag1"
      probe.expectMsg(s"3 $tag1-done")

      readingOurOwnWrites {
        currentTagSeqNrFromDb(pid, tag1).futureValue should ===(Some(2))
      }
      probe.watch(persistentActor)
      persistentActor ! TestActor.Stop
      probe.expectMsg("stopping")
      probe.expectTerminated(persistentActor, 3.seconds)

      val persistentActorIncarnation2 = system.actorOf(TestActor.props(pid))
      persistentActorIncarnation2 ! s"4 $tag1"
      probe.expectMsg(s"4 $tag1-done")
      readingOurOwnWrites {
        currentTagSeqNrFromDb(pid, tag1).futureValue should ===(Some(3))
      }
    }

  }

}
