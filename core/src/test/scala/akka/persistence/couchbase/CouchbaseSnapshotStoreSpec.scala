/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.WithLogCapturing
import com.typesafe.config.ConfigFactory

class CouchbaseSnapshotStoreSpec
    extends SnapshotStoreSpec(
      ConfigFactory
        .parseString(
          """
            |akka.persistence.snapshot-store.plugin = "couchbase-journal.snapshot"
            |akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
          """.stripMargin
        )
        .withFallback(ConfigFactory.load())
    )
    with CouchbaseBucketSetup
    with WithLogCapturing
