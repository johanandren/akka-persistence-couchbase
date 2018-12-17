/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.Inspectors._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class FutureUtilsSpec extends WordSpec with Matchers with ScalaFutures {

  "The future utils" must {

    "allow for sequential traversal" in {
      @volatile var counter = -1

      val result = FutureUtils
        .traverseSequential(0 to 1000)(
          n =>
            Future {
              counter += 1
              (n, counter)
          }
        )
        .futureValue

      forAll(result) {
        case (n, c) =>
          n should ===(c)
      }

    }

    "allow for efficient already-completed-future-traversal" in {
      // no actual testing of how fast it is but since it is identical to
      // the other one you can compare runtime of test case
      @volatile var counter = -1

      val result = FutureUtils
        .traverseSequential(0 to 1000)(
          n =>
            Future.successful {
              counter += 1
              (n, counter)
          }
        )
        .futureValue

      forAll(result) {
        case (n, c) =>
          n should ===(c)
      }
    }
  }

}
