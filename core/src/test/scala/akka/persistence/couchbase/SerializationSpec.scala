/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.persistence.couchbase.internal.CouchbaseSchema.MessageForWrite
import akka.persistence.couchbase.internal.{CouchbaseSchema, SerializedMessage}
import akka.serialization.{AsyncSerializerWithStringManifest, SerializationExtension}
import akka.testkit.TestKit
import com.couchbase.client.java.document.json.JsonObject
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Future

case class MyEvent(n: Int)
case class MyEventAsync(n: Int)
case class MyEventNative(n: Int)

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

class MyEventNativeJsonSerializer extends JsonSerializer {
  override def identifier: Int = 2001
  override def manifest(o: AnyRef): String = o match {
    case _: MyEventNative => "js"
  }
  override def toJson(o: AnyRef): JsonObject = o match {
    case MyEventNative(n) =>
      JsonObject
        .create()
        .put("n", n)
  }
  override def fromJson(json: JsonObject, manifest: String): AnyRef = manifest match {
    case "js" =>
      MyEventNative(json.getInt("n"))
  }
}

class SerializationSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val system = ActorSystem(
    "SerializationSpec",
    ConfigFactory.parseString("""
        akka.actor.serializers.async="akka.persistence.couchbase.MyEventAsyncSerializer"
        akka.actor.serializers.js="akka.persistence.couchbase.MyEventNativeJsonSerializer"
        akka.actor.serialization-bindings {
          "akka.persistence.couchbase.MyEventAsync" = async
          "akka.persistence.couchbase.MyEventNative" = js
        }
      """)
  )
  val serialization = SerializationExtension(system)

  "The serialization of events" must {

    "serialize and deserialize events" in {
      val event = MyEvent(42)
      val serializedEvent = SerializedMessage.serialize(serialization, event).futureValue
      val msgForWrite = new MessageForWrite(23L, serializedEvent)
      val json = CouchbaseSchema.serializedMessageAsJson(msgForWrite)
      val deserialized = SerializedMessage.fromJsonObject(serialization, json).futureValue
      deserialized should ===(event)
    }

    "serialize and deserialize events with an async serializer" in {
      val event = MyEventAsync(42)
      val serializedEvent = SerializedMessage.serialize(serialization, event).futureValue
      val msgForWrite = new MessageForWrite(23L, serializedEvent)
      val json = CouchbaseSchema.serializedMessageAsJson(msgForWrite)
      val deserialized = SerializedMessage.fromJsonObject(serialization, json).futureValue
      deserialized should ===(event)
    }

    "serialize native json" in {
      val event = MyEventNative(42)
      val serializedEvent = SerializedMessage.serialize(serialization, event).futureValue
      val msgForWrite = new MessageForWrite(23L, serializedEvent)
      val json: JsonObject = CouchbaseSchema.serializedMessageAsJson(msgForWrite)
      // serialized form: {"ser_manifest":"js","payload":{"n":42},"ser_id":2001}
      val deserialized = SerializedMessage.fromJsonObject(serialization, json).futureValue
      deserialized should ===(event)
    }

  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}
