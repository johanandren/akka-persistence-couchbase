/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings;

// #init-sourceSingle
import akka.stream.alpakka.couchbase.javadsl.*;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

// #init-sourceSingle

import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.auth.PasswordAuthenticator;
// #init-sourcen1ql
import com.couchbase.client.java.document.ByteArrayDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.SimpleN1qlQuery;

// #init-sourcen1ql

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

// #upsertSingle
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.JsonDocument;
// #upsertSingle

// #upsertFlowSingle
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// #upsertFlowSingle

// #by-single-id-flow
import akka.stream.javadsl.Flow;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
// #by-single-id-flow


/**
 * Compile only samples for the docs
 */
public class Examples {

  // #write-settings
  CouchbaseWriteSettings asyncCouchbaseWriteSettings =
      CouchbaseWriteSettings.create(
          3, // parallelism
          ReplicateTo.THREE,
          PersistTo.TWO,
          Duration.ofSeconds(20)); // write timeout
  // #write-settings

  private ActorSystem system = null;
  private ActorMaterializer materializer = null;


  private void javagetSingleSnippet() throws Exception {

    // #init-actor-system
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    // #init-actor-system

    // #cluster-connect
    // creation is async
    CompletionStage<CouchbaseSession> futureSession = CouchbaseSession.create(
        CouchbaseSessionSettings.create("username", "password"),
        "bucket-name",
        system.dispatcher()
    );
    // for the samples, be careful with blocking in actual code
    CouchbaseSession session = futureSession.toCompletableFuture().get(10, TimeUnit.SECONDS);

    // settings could also come from config
    // expects 'username', 'password' strings and 'nodes' list of strings
    CouchbaseSessionSettings.create(system.settings().config().getConfig("my-app.couchbase"));

    // alternatively from your own setup, note that this is blocking
    CouchbaseCluster cluster = CouchbaseCluster.create("192.168.0.1");
    cluster.authenticate(new PasswordAuthenticator("username", "password"));
    Bucket bucket = cluster.openBucket("bucket-name");
    CouchbaseSession session1 = CouchbaseSession.create(bucket);
    // #cluster-connect

    // #init-sourceSingle
    String id = "First";

    // optional since there may be no such document
    CompletionStage<Optional<JsonDocument>> result = session.get(id);
    // or the raw bytes in case not a json document
    CompletionStage<Optional<ByteArrayDocument>> rawResult = session.get(id, ByteArrayDocument.class);
    // #init-sourceSingle
  }

  private void javaBulkSnippet() {
    CouchbaseSession session = null;

    // #init-sourceBulk
    List<String> ids = Arrays.asList("One", "Two", "Three");

    CompletionStage<List<JsonDocument>> result  =
        Source.from(ids)
          .mapAsync(1, id -> session.get(id))
          .filter(doc -> doc.isPresent())
          .map(doc -> doc.get())
          .runWith(Sink.seq(), materializer);
    // #init-sourceBulk
  }

  private void javaN1QLSnippet() {

    CouchbaseSession session = null;

    // #init-sourcen1ql
    N1qlParams params = N1qlParams.build().adhoc(false);
    SimpleN1qlQuery query = N1qlQuery.simple("select count(*) from akkaquery", params);

    Source<JsonObject, NotUsed> source = session.streamedQuery(query);

    CompletionStage<List<JsonObject>> result = source.runWith(Sink.seq(), materializer);
    // #init-sourcen1ql
  }

  private void upsertSnippet() {
    CouchbaseSession session = null;
    // #upsertSingle

    JsonDocument document =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));

    CompletionStage<JsonDocument> upsertDone = session.upsert(document);
    // #upsertSingle

    // #upsertBulk


    JsonDocument document1 =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));
    JsonDocument document2 =
        JsonDocument.create("id2", JsonObject.create().put("value", "some other Value"));

    Source<JsonDocument, NotUsed> docs = Source.from(Arrays.asList(document1, document2));

    // in a stream using mapAsync
    Flow<JsonDocument, JsonDocument, NotUsed> upsertFlow =
        Flow.of(JsonDocument.class)
            .mapAsync(1, doc -> session.upsert(doc));

    CompletionStage<Done> result =
        docs.via(upsertFlow).runWith(Sink.ignore(), materializer);
    // #upsertBulk
  }

  private void DeleteSingleSinkSnippet() {
    CouchbaseSession session = null;

    // #delete-single-sink
    CompletionStage<Done> removeDone = session.remove("id");
    // #delete-single-sink

    // #delete-bulk-sink
    String id = "id1";
    String id2 = "id2";
    Source<String, NotUsed> ids = Source.from(Arrays.asList(id, id2));

    Sink<String, CompletionStage<Done>> sink =
        Flow.of(String.class)
          .mapAsync(1, idToRemove -> session.remove(idToRemove))
          .toMat(Sink.ignore(), Keep.right());

    CompletionStage<Done> result = ids.runWith(sink, materializer);
    // #delete-bulk-sink
  }

  private void UpsertSingleFlowSnippet() {
    CouchbaseSession session = null;

    // #upsertFlowSingle
    JsonDocument document =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));

    CompletionStage<JsonDocument> done = session.upsert(document);

    // in a flow
    Source.single(document)
        .mapAsync(1, session::upsert)
        .runWith(Sink.ignore(), materializer);
    // #upsertFlowSingle
  }

  private void getByIdFlowSnippet() {
    CouchbaseSession session = null;

    // #by-single-id-flow
    String id = "First";
    CompletionStage<Optional<JsonDocument>> result = session.get(id);
    // #by-single-id-flow

    // #by-bulk-id-flow
    List<String> ids = Arrays.asList("id1", "id2", "id3", "id4");

    Source<String, NotUsed> source = Source.from(ids);
    Flow<String, JsonDocument, NotUsed> flow =
        Flow.of(String.class)
            .mapAsync(1, session::get)
        .filter(doc -> doc.isPresent())
        .map(doc -> doc.get());

    CompletionStage<List<JsonDocument>> docs =
        source.via(flow).runWith(Sink.seq(), materializer);
    // #by-bulk-id-flow
  }


  private void DeleteBulFlowSnippet() {
    CouchbaseSession session = null;

    // #delete-bulk-flow
    Source<String, NotUsed> ids = Source.from(Arrays.asList("id1", "id2"));

    Flow<String, String, NotUsed> removeFlow =
        Flow.of(String.class)
          .mapAsync(1,
              id ->
                session.remove(id).thenApply(done -> id)
              );

    CompletionStage<List<String>> removedIds = ids.via(removeFlow).runWith(Sink.seq(), materializer);
    // #delete-bulk-flow
  }
}