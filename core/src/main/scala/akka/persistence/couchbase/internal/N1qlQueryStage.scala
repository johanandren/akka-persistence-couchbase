/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal

import akka.annotation.InternalApi
import akka.persistence.couchbase.CouchbaseReadJournalSettings
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, AsyncN1qlQueryRow, N1qlQuery}
import rx.functions.Func1
import rx.{Observable, Subscriber}

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object N1qlQueryStage {
  trait Control
  case object Poll

  sealed trait InternalState
  final case object Idle extends InternalState
  final case object IdleAfterFullPage extends InternalState
  final case object Querying extends InternalState

  val unfoldRows = new Func1[AsyncN1qlQueryResult, Observable[AsyncN1qlQueryRow]] {
    def call(t: AsyncN1qlQueryResult): Observable[AsyncN1qlQueryRow] =
      t.rows()
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class N1qlQueryStage[S](live: Boolean,
                                            settings: CouchbaseReadJournalSettings,
                                            initialQuery: N1qlQuery,
                                            bucket: AsyncBucket,
                                            initialState: S,
                                            nextQuery: S => Option[N1qlQuery],
                                            updateState: (S, AsyncN1qlQueryRow) => S)
    extends GraphStageWithMaterializedValue[SourceShape[AsyncN1qlQueryRow], N1qlQueryStage.Control] {

  import N1qlQueryStage._

  val out: Outlet[AsyncN1qlQueryRow] = Outlet("LiveN1qlQuery.out")
  override def shape: SourceShape[AsyncN1qlQueryRow] = SourceShape(out)

  class N1qlQueryStageLogic extends TimerGraphStageLogicWithLogging(shape) with OutHandler with Control {
    var currentState: S = initialState
    var rowsInCurrentQuery = 0
    // TODO use a mutable buffer e.g. ArrayDeque
    var buffer = Vector.empty[AsyncN1qlQueryRow]
    var state: InternalState = Idle

    private val newRowCb = getAsyncCallback[AsyncN1qlQueryRow] { row =>
      currentState = updateState(currentState, row)
      log.debug("New row: {}. Updating state to {}", row, currentState)
      rowsInCurrentQuery += 1
      buffer = buffer :+ row

      tryPush()
    }

    private val completeCb = getAsyncCallback[Unit] { _ =>
      log.debug("Query complete. Remaining buffer: {}", buffer)
      if (rowsInCurrentQuery == settings.pageSize)
        state = IdleAfterFullPage
      else
        state = Idle
      if (buffer.isEmpty) {
        if (live) {
          // continue until we don't get a full page
          // TODO alternative would be to more aggressively query next until we get empty result
          if (rowsInCurrentQuery == settings.pageSize)
            doNextQuery()
          else {
            // wait for timer
          }
        } else {
          // non-live, continue until we don't get a full page
          if (rowsInCurrentQuery == settings.pageSize)
            doNextQuery()
          else
            completeStage()
        }
      } else {
        tryPush()
      }
      rowsInCurrentQuery = 0
    }

    private val failedCb = getAsyncCallback[Throwable] { t =>
      log.error(t, "Query failed")
      failStage(t)
    }

    override def preStart(): Unit = {
      if (live)
        schedulePeriodicallyWithInitialDelay(Poll, settings.liveQueryInterval, settings.liveQueryInterval)
      executeQuery(initialQuery)
    }

    override protected def onTimer(timerKey: Any): Unit = timerKey match {
      case Poll =>
        state match {
          case Idle | IdleAfterFullPage =>
            log.debug("Poll when idle")
            if (buffer.isEmpty) {
              doNextQuery()
            } else {
              tryPush()
            }
          case Querying =>
            log.debug("Query already outstanding. Ignoring poll.")
        }
    }

    private def doNextQuery(): Unit = {
      require(state == Idle || state == IdleAfterFullPage)
      nextQuery(currentState) match {
        case Some(next) =>
          log.debug("doNextQuery {}", next)
          executeQuery(next)

        case None =>
          log.debug("doNextQuery - finished")
          completeStage()
      }
    }

    private def executeQuery(query: N1qlQuery): Unit = {
      state = Querying
      // FIXME deal with initial errors
      log.debug("Executing query {}", query)
      bucket
        .query(query)
        .flatMap(N1qlQueryStage.unfoldRows)
        .subscribe(new Subscriber[AsyncN1qlQueryRow]() {
          override def onCompleted(): Unit = completeCb.invoke(())
          override def onError(t: Throwable): Unit = failedCb.invoke(t)
          override def onNext(row: AsyncN1qlQueryRow): Unit = newRowCb.invoke(row)
        })
    }

    override def onPull(): Unit = {
      log.debug("onPull {}", buffer)
      tryPush()

      if (!live && buffer.isEmpty) {
        state match {
          case IdleAfterFullPage =>
            doNextQuery()
          case Idle =>
            completeStage
          case Querying => // more in flight
        }
      }
    }

    private def tryPush(): Unit = {
      log.debug("tryPush {}", buffer)
      if (isAvailable(out)) {
        buffer match {
          case head +: tail =>
            push(out, head)
            buffer = tail
          case _ => // wait for next row
        }
      }
    }

    setHandler(out, this)
  }

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, N1qlQueryStage.Control) = {
    val logic = new N1qlQueryStageLogic()
    (logic, logic)
  }
}
