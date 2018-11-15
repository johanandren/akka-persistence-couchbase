/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.serialization.{AsyncSerializerWithStringManifest, SerializationExtension}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Future

case class MyEvent(n: Int)
case class MyEventAsync(n: Int)

class MyEventAsyncSerializer(system: ExtendedActorSystem) extends AsyncSerializerWithStringManifest(system) {

  import system.dispatcher

  override def identifier: Int = 1984
  override def manifest(o: AnyRef): String = o match {
    case _: MyEventAsync => "Woo!"
  }
  override def toBinaryAsync(o: AnyRef): Future[Array[Byte]] =
    Future {
      o match {
        case MyEventAsync(n) => BigInt(n).toByteArray
      }
    }
  override def fromBinaryAsync(bytes: Array[Byte], manifest: String): Future[AnyRef] =
    Future {
      manifest match {
        case "Woo!" => MyEventAsync(BigInt(bytes).toInt)
      }
    }

}

class SerializationSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val system = ActorSystem(
    "SerializationSpec",
    ConfigFactory.parseString(
      """
    akka.actor.serializers.async="akka.persistence.couchbase.MyEventAsyncSerializer"
    akka.actor.serialization-bindings {
      "akka.persistence.couchbase.MyEventAsync" = async
    }
    """
    )
  )
  val serialization = SerializationExtension(system)

  "The serialization of events" must {

    "serialize and deserialize events" in {
      val event = MyEvent(42)
      val serializedEvent = SerializedMessage.serialize(serialization, event).futureValue
      val json = CouchbaseSchema.serializedMessageToObject(serializedEvent)
      val deserialized = SerializedMessage.fromJsonObject(serialization, json).futureValue
      deserialized should ===(event)
    }

    "serialize and deserialize events with an async serializer" in {
      val event = MyEventAsync(42)
      val serializedEvent = SerializedMessage.serialize(serialization, event).futureValue
      val json = CouchbaseSchema.serializedMessageToObject(serializedEvent)
      val deserialized = SerializedMessage.fromJsonObject(serialization, json).futureValue
      deserialized should ===(event)
    }

  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}
