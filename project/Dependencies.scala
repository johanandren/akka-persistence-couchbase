/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {

  val AkkaVersion = "2.5.15"
  val LagomVersion = "1.5.0-M3"

  object Compile {
    val couchbaseClient = "com.couchbase.client" % "java-client" % "2.7.0" // Apache V2
    val couchbaseDcpClient = "com.couchbase.client" % "dcp-client" % "0.19.0" // Apache V2

    // used to easily convert rxjava into reactive streams and then into akka streams
    val rxJavaReactiveStreams = "io.reactivex" % "rxjava-reactive-streams" % "1.2.1" // Apache V2

    val akkaActor = "com.typesafe.akka" %% "akka-actor" % AkkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence" % AkkaVersion
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion

    val lagomJavaDslApi = "com.lightbend.lagom" %% "lagom-javadsl-api" % LagomVersion
    val lagomScalaDslApi = "com.lightbend.lagom" %% "lagom-scaladsl-api" % LagomVersion
    val lagomPersistenceCore = "com.lightbend.lagom" %% "lagom-persistence-core" % LagomVersion
    val lagomPersistenceScalaDsl = "com.lightbend.lagom" %% "lagom-scaladsl-persistence" % LagomVersion
    val lagomPersistenceJavaDsl = "com.lightbend.lagom" %% "lagom-javadsl-persistence" % LagomVersion
    val lagomPlayJson = "com.lightbend.lagom" %% "lagom-scaladsl-play-json" % LagomVersion
    val lagomJavaDslJackson = "com.lightbend.lagom" %% "lagom-javadsl-jackson" % LagomVersion

    val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.25"
  }

  object Test {
    val akkaPersistenceTck = "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % "test"
    val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % "test"
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % "test"
    val akkaMultiNodeTestkit = "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion % "test"

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % "test" // EPL 1.0 / LGPL 2.1
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % "test" // ApacheV2
    val junit = "junit" % "junit" % "4.12" % "test"
    val junitInterface = "com.novocode" % "junit-interface" % "0.11" % "test"

    val lagomTestKitScalaDsl = "com.lightbend.lagom" %% "lagom-scaladsl-testkit" % LagomVersion % "test"
    val lagomTestKitJavaDsl = "com.lightbend.lagom" %% "lagom-javadsl-testkit" % LagomVersion % "test"
    val lagomPersistenceTestKit = "com.lightbend.lagom" %% "lagom-persistence-testkit" % LagomVersion % "test"
  }

  import Compile._
  import Test._

  val core = Seq(
    akkaActor,
    akkaPersistence,
    akkaPersistenceQuery,
    akkaPersistenceTck,
    akkaStreamTestkit,
    akkaTestkit,
    logback,
    slf4jApi,
    scalaTest
  )

  val couchbaseClient = Seq(
    akkaActor,
    akkaStream,
    akkaStreamTestkit,
    Compile.couchbaseClient,
    couchbaseDcpClient,
    rxJavaReactiveStreams,
    scalaTest,
    junit,
    junitInterface,
    logback
  )

  val `copy-of-lagom-persistence-test` = Seq(
    lagomPersistenceCore,
    lagomPersistenceScalaDsl,
    lagomPersistenceJavaDsl,
    akkaTestkit,
    scalaTest,
    akkaMultiNodeTestkit
  )

  val `lagom-persistence-couchbase-core` = Seq(
    lagomPersistenceCore,
    slf4jApi,
    scalaTest
  )

  val `lagom-persistence-couchbase-scaladsl` = Seq(
    lagomPersistenceCore,
    lagomPersistenceScalaDsl,
    lagomScalaDslApi,
    scalaTest
  )

  val `lagom-persistence-couchbase-javadsl` = Seq(
    lagomPersistenceCore,
    lagomPersistenceJavaDsl,
    lagomJavaDslApi,
    junit,
    junitInterface
  )

}
