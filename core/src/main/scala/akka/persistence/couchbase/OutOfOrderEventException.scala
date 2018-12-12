/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

/**
 * Thrown when an out of order event is detected in event streams
 */
final class OutOfOrderEventException(msg: String) extends RuntimeException(msg)
