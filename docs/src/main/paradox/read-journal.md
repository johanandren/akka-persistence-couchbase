# Query Plugin

## Features

The query plugin provides all the Akka Persistence Query [Read Journal plugin APIs](https://doc.akka.io/docs/akka/current/persistence-query.html#readjournal-plugin-api):

 * `PersistenceIdsQuery` and `CurrentPersistenceIdsQuery` for querying and following what persistence ids exist in the journal
 * `CurrentEventsByPersistenceIdQuery` and `EventsByPersistenceIdQuery` for querying and following events from a specific persistent actor  
 * `CurrentEventsByTagQuery` and `EventsByTagQuery` for querying and following tagged events

## Configuration

For setting up the project to use the plugin, and preparing the couchbase bucket, see [Getting Started](getting-started.md)
and especially the tag-related queries if you intend to use the tagging queries.

Accessing the (default) read journal can be done through an extension

Scala
:  @@snip(/core/src/test/scala/akka/persistence/couchbase/scaladsl/AbstractQuerySpec.scala) { #read-journal-access }  

Java
:  @@snip(/core/src/test/java/akka/persistence/couchbase/javadsl/CouchbaseReadJournalTest.java) { #read-journal-access }

The connection settings are shared with the journal plugin, see [Journal](journal.md). 
 
See [reference.conf](https://github.com/akka/akka-persistence-couchbase/blob/master/core/src/main/resources/reference.conf) 
for complete configuration option docs and defaults.

## Persistence IDs

The persistence ids are delivered in no specific order, there is no guarantee that two calls will result in the same
order of persistence ids. A single persistence id will only ever be in the result stream once. Newly written ids may
not immediately be visible (see caveat about eventual consistency below). 

## Events by Persistence ID

The events by persistence id query streams all events for a given persistence id in the order they were written 
(guaranteed by the sequence numbering of events per persistent actor). 
Newly written events may not be immediately visible in Couchbase (see caveat about eventual consistency below)

## Events by Tag 

The event by tag stream is globally sorted using a time based UUID, this means it is very important that the Akka
nodes are kept in tight clock sync. 

### Eventual Consistency
Even with clocks in sync, writing to different nodes of the couchbase cluster
could mean that the change reaches the query index out of order, to protect against missing events because of this
or small clock skew the queries will never read the latest events but only up to a point in time a little while ago
giving better chance that the index is consistent with the written data.

The offset back in time is configured through the setting `couchbase-journal.read.events-by-tag.eventual-consistency-delay`.
By default it is set to 5 seconds, this means that at any time a query is performed it will only see events up until
5 seconds ago. For the live query it will keep catching up to 5 seconds ago as time passes by. 

If the `eventual-consistency-delay` is tuned too low or clock skew larger than it happens it could mean that tagged 
events are seen out of order, to detect this the tagged events have a gap free sequence number per persistence id, 
when streaming events and an out of order event is detected the stream is failed with an `akka.persistence.couchbase.OutOfOrderEventException`.

In a scenario where a projection is created from the streamed events this could mean the projection needs to be discarded
and recreated.

### Events by tag offsets

The event by tag stream supports using `akka.persistence.query.TimeBasedUUID`s to offset into stream of tagged events,
for example to follow a stream and update a projection. Sequence numbers for offsets are not supported.

Offsets can be created from unix timestamps using `akka.persistence.couchbase.UUIDs.timeBasedUUIDFrom(unixTimestampMs)`.

## Caveats

 * As the indexes used to perform the queries are eventually consistent (even for a single writer node) there 
   is no guarantee that an immediate query will see the latest writes.
 