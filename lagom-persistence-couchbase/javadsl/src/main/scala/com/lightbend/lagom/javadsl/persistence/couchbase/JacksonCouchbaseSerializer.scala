/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase
import java.io.{InputStream, NotSerializableException}
import java.math.BigInteger

import akka.actor.ExtendedActorSystem
import akka.persistence.couchbase.JsonSerializer
import com.couchbase.client.java.document.json.{JsonArray, JsonObject, JsonValue}
import com.fasterxml.jackson.core._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import com.lightbend.lagom.internal.jackson.{JacksonJsonSerializer, JacksonObjectMapperProvider}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

object JacksonCouchbaseSerializer {

  def toCouchbase(jobj: ObjectNode): JsonObject = {
    val cobj = JsonObject.create()
    jobj.fields().asScala.foreach { field =>
      field.getValue match {
        case nestedJobj: ObjectNode =>
          val cobj = toCouchbase(nestedJobj)
          cobj.put(field.getKey, cobj)

        case nestedJarray: ArrayNode =>
          val carray = toCouchbase(nestedJarray)
          cobj.put(field.getKey, carray)

        case jint: IntNode =>
          cobj.put(field.getKey, jint.intValue())

        case jfloat: FloatNode =>
          cobj.put(field.getKey, jfloat.floatValue())

        case jdouble: DoubleNode =>
          cobj.put(field.getKey, jdouble.doubleValue())

        case jstring: TextNode =>
          cobj.put(field.getKey, jstring.textValue())

        case jbool: BooleanNode =>
          cobj.put(field.getKey, jbool.booleanValue())

      }
    }
    cobj
  }

  def toCouchbase(jarray: ArrayNode): JsonArray = {
    val carray = JsonArray.create()
    jarray.iterator().asScala.foreach { jentry =>
      jentry match {
        case nestedJobj: ObjectNode =>
          carray.add(toCouchbase(nestedJobj))

        case nestedJarray: ArrayNode =>
          val nestedCarray = toCouchbase(nestedJarray)
          carray.add(nestedCarray)

        case jint: IntNode =>
          carray.add(jint.intValue())

        case jfloat: FloatNode =>
          carray.add(jfloat.floatValue())

        case jdouble: DoubleNode =>
          carray.add(jdouble.doubleValue())

        case jstring: TextNode =>
          carray.add(jstring.textValue())

        case jbool: BooleanNode =>
          carray.add(jbool.booleanValue())
      }
    }
    carray
  }

  def toJsonNode(any: Any, nodeFactory: JsonNodeFactory): JsonNode = any match {
    case nestedCobj: JsonObject =>
      fromCouchbase(nestedCobj, nodeFactory)
    case carray: JsonArray =>
      fromCouchbase(carray, nodeFactory)
    case n: Int =>
      nodeFactory.numberNode(n)
    case d: Double =>
      nodeFactory.numberNode(d)
    case f: Float =>
      nodeFactory.numberNode(f)
    case b: Boolean =>
      nodeFactory.booleanNode(b)
    case t: String =>
      nodeFactory.textNode(t)
  }

  def fromCouchbase(carray: JsonArray, nodeFactory: JsonNodeFactory): JsonNode = {
    val items = carray
      .iterator()
      .asScala
      .map { item =>
        toJsonNode(item.asInstanceOf[Any], nodeFactory)
      }
      .toVector
      .asJava

    nodeFactory
      .arrayNode()
      .addAll(items)
  }

  def fromCouchbase(cobj: JsonObject, nodeFactory: JsonNodeFactory): JsonNode = {

    val fields =
      cobj.getNames.asScala
        .map { fieldName =>
          fieldName -> toJsonNode((cobj.get(fieldName).asInstanceOf[Any]), nodeFactory)
        }
        .toMap
        .asJava

    val objectNode = nodeFactory.objectNode()
    objectNode.setAll(fields)
  }
}

class JacksonCouchbaseSerializer(as: ExtendedActorSystem) extends JsonSerializer {

  import JacksonCouchbaseSerializer._

  // FIXME if this was to be actually used we need the same kind of migration infra as the JacksonJsonSerializer from lagom

  val objectMapper = JacksonObjectMapperProvider(as).objectMapper

  override val identifier: Int = 777

  def manifest(o: AnyRef): String = o.getClass.getName

  def toJson(o: AnyRef): JsonObject = {
    val jsonNode = objectMapper.valueToTree(o)
    jsonNode match {
      case jobj: ObjectNode => toCouchbase(jobj)
      case jarr: ArrayNode => toCouchbase(jarr)
    }
  }
  def fromJson(json: JsonObject, manifest: String): AnyRef = {
    val nodeFactory = objectMapper.getNodeFactory
    val tree = fromCouchbase(json, nodeFactory)
    val clazz = as.dynamicAccess.getClassFor[AnyRef](manifest) match {
      case Success(clazz) ⇒ clazz
      case Failure(e) ⇒
        throw new NotSerializableException(
          s"Cannot find manifest class [$manifest] for serializer [${getClass.getName}]."
        )
    }
    objectMapper.treeToValue(tree, clazz)
  }

}
