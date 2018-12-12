/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal
import akka.annotation.InternalApi

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object FutureUtils {

  /**
   * Like Future.traverse but invokes `toFuture` on one A at a time, and does not execute the next one until
   * the returned `Future[B]` completes. Simplification of utilities in:
   * https://github.com/johanandren/futiles/blob/master/src/main/scala/markatta/futiles/Traversal.scala#L27
   */
  def traverseSequential[A, B](
      as: immutable.Seq[A]
  )(toFuture: A => Future[B])(implicit ec: ExecutionContext): Future[immutable.Seq[B]] =
    foldLeftSequential(as)(Nil: List[B])((bs, a) => toFuture(a).map(b => b :: bs)).map(_.reverse)

  /**
   * Fold over `as` but invoke `toNextFuture` on one `A` at a time, waiting for it to complete before the next
   * `A` is folded.
   */
  def foldLeftSequential[A, B](
      as: immutable.Seq[A]
  )(zero: B)(toNextFuture: (B, A) => Future[B])(implicit ec: ExecutionContext): Future[B] =
    if (as.isEmpty) Future.successful(zero)
    else toNextFuture(zero, as.head).flatMap(b => foldLeftSequential(as.tail)(b)(toNextFuture))

}
