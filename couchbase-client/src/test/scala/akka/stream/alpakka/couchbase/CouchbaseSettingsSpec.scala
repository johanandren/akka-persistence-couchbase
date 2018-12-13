/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

import java.time.{Duration => JDuration}
import com.couchbase.client.java.{PersistTo, ReplicateTo}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._

class CouchbaseSettingsSpec extends WordSpec with Matchers {

  "The couchbase session settings" must {

    "parse config into settings" in {
      val config = ConfigFactory.parseString("""
          username=scott
          password=tiger
          nodes=["example.com:2323"]
        """)

      val settingss = Seq(CouchbaseSessionSettings(config), CouchbaseSessionSettings.create(config))
      settingss.foreach { settings =>
        settings.username should ===("scott")
        settings.password should ===("tiger")
        settings.nodes should ===(List("example.com:2323"))
      }
    }

    "be changeable using 'with' methods" in {
      val modified = CouchbaseSessionSettings("scott", "tiger")
        .withNodes("example.com:123")
        .withUsername("bob")
        .withPassword("lynx")

      modified.username should ===("bob")
      modified.password should ===("lynx")
      modified.nodes should ===("example.com:123" :: Nil)
    }

    "be equal based on the fields" in { // prerequisite for the session registry
      val a = CouchbaseSessionSettings("scott", "tiger")
        .withNodes("example.com:123")
      val b = CouchbaseSessionSettings("scott", "tiger")
        .withNodes("example.com:123")

      a should ===(b)
      a.hashCode === (b.hashCode())
    }

  }

  "The couchbase write settings" must {

    "be created from both java and scala APIs" in {
      val scala = CouchbaseWriteSettings(1, ReplicateTo.NONE, PersistTo.THREE, 10.seconds)
      val java =
        CouchbaseWriteSettings.create(1, ReplicateTo.NONE, PersistTo.THREE, JDuration.ofSeconds(10))

      scala.parallelism should ===(java.parallelism)
      scala.persistTo should ===(java.persistTo)
      scala.replicateTo should ===(java.replicateTo)
      scala.timeout.toMillis should ===(java.timeout.toMillis)
    }

  }

}
