/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object CouchbaseWriteSettings {

  def apply(parallelism: Int,
            replicateTo: ReplicateTo,
            persistTo: PersistTo,
            timeout: FiniteDuration): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout)

  def create(parallelism: Int,
             replicateTo: ReplicateTo,
             persistTo: PersistTo,
             timeout: java.time.Duration): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism,
                               replicateTo,
                               persistTo,
                               FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS))

}

final class CouchbaseWriteSettings private (val parallelism: Int,
                                            val replicateTo: ReplicateTo,
                                            val persistTo: PersistTo,
                                            val timeout: FiniteDuration) {

  def withParallelism(parallelism: Int): CouchbaseWriteSettings = copy(parallelism = parallelism)

  def withReplicateTo(replicateTo: ReplicateTo): CouchbaseWriteSettings = copy(replicateTo = replicateTo)

  def withPersistTo(persistTo: PersistTo): CouchbaseWriteSettings = copy(persistTo = persistTo)

  /**
   * Java API:
   */
  def withTimeOut(timeout: java.time.Duration): CouchbaseWriteSettings =
    copy(timeout = FiniteDuration(timeout.toMillis, TimeUnit.MILLISECONDS))

  /**
   * Scala API:
   */
  def withTimeout(timeout: FiniteDuration): CouchbaseWriteSettings = copy(timeout = timeout)

  private[this] def copy(parallelism: Int = parallelism,
                         replicateTo: ReplicateTo = replicateTo,
                         persistTo: PersistTo = persistTo,
                         timeout: FiniteDuration = timeout) =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout)

  override def toString: String = s"CouchbaseWriteSettings($parallelism, $replicateTo, $persistTo, $timeout)"
}

object CouchbaseSessionSettings {

  /**
   * Scala API: Load the session from the given config object, expects the config object to have the fields `username`,
   * `password` and `nodes`. Using it means first looking your config namespace up yourself using `config.getConfig("some.path")`.
   */
  def apply(config: Config): CouchbaseSessionSettings = {
    val username = config.getString("username")
    val password = config.getString("password")
    val nodes = config.getStringList("nodes").asScala.toList
    new CouchbaseSessionSettings(username, password, nodes, None)
  }

  /**
   * Scala API:
   */
  def apply(username: String, password: String): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, Nil, None)

  /**
   * Java API:
   */
  def create(username: String, password: String): CouchbaseSessionSettings =
    apply(username, password)

  /**
   * Java API: Load the session from the given config object, expects the config object to have the fields `username`,
   * `password` and `nodes`. Using it means first looking your config namespace up yourself using `config.getConfig("some.path")`.
   */
  def create(config: Config): CouchbaseSessionSettings = apply(config)

}

final class CouchbaseSessionSettings private (val username: String,
                                              val password: String,
                                              val nodes: immutable.Seq[String],
                                              val environment: Option[CouchbaseEnvironment]) {

  def withUsername(username: String): CouchbaseSessionSettings =
    copy(username = username)

  def withPassword(password: String): CouchbaseSessionSettings =
    copy(password = password)

  def withNodes(nodes: String): CouchbaseSessionSettings =
    copy(nodes = nodes :: Nil)

  def withNodes(nodes: immutable.Seq[String]): CouchbaseSessionSettings =
    copy(nodes = nodes)

  def withNodes(nodes: java.util.List[String]): CouchbaseSessionSettings =
    copy(nodes = nodes.asScala.toList)

  def withEnvironment(environment: CouchbaseEnvironment): CouchbaseSessionSettings =
    copy(environment = Some(environment))

  private def copy(username: String = username,
                   password: String = password,
                   nodes: immutable.Seq[String] = nodes,
                   environment: Option[CouchbaseEnvironment] = environment): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, nodes, environment)

  override def equals(other: Any): Boolean = other match {
    case that: CouchbaseSessionSettings =>
      username == that.username &&
      password == that.password &&
      nodes == that.nodes &&
      environment == that.environment
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(username, password, nodes, environment)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"CouchbaseSessionSettings($username, *****, ${nodes.mkString("[", ", ", "]")}, $environment)"
}
