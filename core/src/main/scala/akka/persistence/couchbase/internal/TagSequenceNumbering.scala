/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal
import java.util.concurrent.ConcurrentHashMap

import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.persistence.couchbase.internal.CouchbaseSchema.{Fields, Queries}
import com.couchbase.client.java.query.N1qlParams

import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi private[akka] trait TagSequenceNumbering { self: AsyncCouchbaseSession with Queries =>

  type Tag = String
  type PersistenceId = String
  protected implicit def executionContext: ExecutionContext
  protected def log: LoggingAdapter
  protected def queryConsistency: N1qlParams
  private val taggingPerPidSequenceNumbers = new ConcurrentHashMap[(Tag, PersistenceId), java.lang.Long]()

  def putSeqNr(pid: PersistenceId, tag: Tag, seqNr: Long) =
    taggingPerPidSequenceNumbers.put((pid, tag), seqNr)

  /**
   * Return the in-mem seq-nr for a tag or load the highest seq nr for the tag from the db.
   * Precondition for thread safety: there will only ever be one concurrent call for the same pid and tag
   * (different tag/pid is fine though)
   */
  def nextTagSeqNrFor(pid: PersistenceId, tag: Tag): Future[Long] = {
    val key = (pid, tag)
    Option(taggingPerPidSequenceNumbers.get(key)) match {

      case Some(present) =>
        Future.successful(present)
        val newValue = present + 1
        val replaced = taggingPerPidSequenceNumbers.put(key, newValue)
        if (replaced == present)
          Future.successful(newValue)
        else
          throw new IllegalStateException(s"Saw concurrent update for $key, not allowed")

      case None =>
        currentTagSeqNrFromDb(pid, tag)
          .map {
            case Some(tagSeqNr) =>
              log.debug("Got tagSeqNr {} from database for actor [{}] and tag [{}], fetching from database",
                        tagSeqNr,
                        pid,
                        tag)
              tagSeqNr + 1
            case None =>
              log.debug("No previous tagSeqNr for actor [{}] and tag [{}], starting from 1", pid, tag)
              1L
          }
          .map(
            nextTagSeqNr =>
              if (taggingPerPidSequenceNumbers.put(key, nextTagSeqNr) == null)
                nextTagSeqNr
              else
                throw new IllegalStateException(s"Saw concurrent update for $key, not allowed")
          )
    }
  }

  /**
   * Remove all entries for `pid`. It is expected that no updates for the specific persistence id happens until
   * this method has returned.
   */
  def evictSeqNrsFor(pid: PersistenceId): Unit = {
    val keys = taggingPerPidSequenceNumbers.keySet.iterator()
    while (keys.hasNext) {
      val key @ (keyPid, _) = keys.next()
      if (keyPid == pid)
        keys.remove()
    }
  }

  protected def currentTagSeqNrFromDb(pid: PersistenceId, tag: Tag): Future[Option[Long]] =
    withCouchbaseSession { session =>
      session.singleResponseQuery(highestTagSequenceNumberQuery(pid, tag, queryConsistency)).map {
        case Some(json) => Some(json.getLong(Fields.TagSeqNr))
        case None => None
      }
    }

}
