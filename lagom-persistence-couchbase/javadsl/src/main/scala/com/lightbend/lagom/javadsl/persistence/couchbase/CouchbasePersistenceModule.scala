/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import java.net.URI

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import com.google.inject.Provider
import com.lightbend.lagom.internal.javadsl.persistence.couchbase.{
  CouchbasePersistentEntityRegistry,
  CouchbaseReadSideImpl,
  JavadslCouchbaseOffsetStore
}
import com.lightbend.lagom.internal.persistence.couchbase.{
  CouchbaseConfigValidator,
  CouchbaseOffsetStore,
  ServiceLocatorAdapter,
  ServiceLocatorHolder
}
import com.lightbend.lagom.javadsl.api.ServiceLocator
import com.lightbend.lagom.javadsl.persistence.PersistentEntityRegistry
import com.lightbend.lagom.spi.persistence.OffsetStore
import com.typesafe.config.Config
import javax.annotation.PostConstruct
import javax.inject.Inject
import play.api.inject.{Binding, Injector, Module}
import play.api.{Configuration, Environment}

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Guice module for the Persistence API.
 */
class CouchbasePersistenceModule extends Module {

  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] = Seq(
    bind[CouchbasePersistenceModule.InitServiceLocatorHolder].toSelf.eagerly(),
    bind[PersistentEntityRegistry].to[CouchbasePersistentEntityRegistry],
    bind[CouchbaseSession].toProvider[CouchbaseProvider],
    bind[CouchbaseReadSide].to[CouchbaseReadSideImpl],
    //TODO: add other modules similar to Cassandra
    //    bind[CassandraReadSideSettings].toSelf,
    bind[CouchbaseOffsetStore].to(bind[JavadslCouchbaseOffsetStore]),
    bind[OffsetStore].to(bind[CouchbaseOffsetStore])
  )

}

private[lagom] class CouchbaseProvider @Inject()(system: ActorSystem, cfg: Config) extends Provider[CouchbaseSession] {

  private val log = Logging(system, classOf[CouchbaseProvider])

  CouchbaseConfigValidator.validateBucket("lagom.persistence.read-side.couchbase", cfg, log)

  private val readSideCouchbaseConfig: Config =
    cfg.getConfig("lagom.persistence.read-side.couchbase")

  private val sessionSettings = CouchbaseSessionSettings(
    readSideCouchbaseConfig.getConfig("connection")
  )

  private val bucket = readSideCouchbaseConfig.getString("bucket")

  // FIXME is there a way to have async component creation in lagom instead of letting every component know that the thing is async?
  // if not we should pass Future[CouchbaseSession] around and let the use sites mix in AsyncCouchbaseSession - but if we use
  // that from Lagom it needs to be made public API
  // FIXME this should be the Java API of CouchbaseSession, when there is one
  lazy val couchbase: CouchbaseSession =
    Await.result(CouchbaseSession.create(sessionSettings, bucket, system.dispatcher).toScala, 30.seconds)

  override def get(): CouchbaseSession = couchbase
}

private[lagom] object CouchbasePersistenceModule {

  class InitServiceLocatorHolder @Inject()(system: ActorSystem, injector: Injector) {

    // Guice doesn't support this, but other DI frameworks do.
    @PostConstruct
    def init(): Unit =
      Try(injector.instanceOf[ServiceLocator]).foreach { locator =>
        ServiceLocatorHolder(system).setServiceLocator(new ServiceLocatorAdapter {
          override def locateAll(name: String): Future[List[URI]] = {
            import system.dispatcher

            import scala.collection.JavaConverters._
            import scala.compat.java8.FutureConverters._
            locator.locateAll(name).toScala.map(_.asScala.toList)
          }
        })
      }
  }

}
