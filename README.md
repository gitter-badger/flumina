Flumina
===

[![Build Status](https://travis-ci.org/vectos/flumina.svg)](https://travis-ci.org/vectos/flumina)
[![Coverage Status](https://coveralls.io/repos/github/vectos/flumina/badge.svg?branch=master)](https://coveralls.io/github/vectos/flumina?branch=master)
[![Dependencies](https://app.updateimpact.com/badge/762391907245625344/flumina.svg?config=runtime)](https://app.updateimpact.com/latest/762391907245625344/flumina)

Kafka driver written in pure Scala. Request and response messages and their respective codecs reside in the `types` project. Currently we support version 0 only. This is due testing of the protocol. We plan to support version 1, 2 and future versions (this will be copy-pasting and editing some stuff).

## Disclaimer

The library should be treated as alpha software. Be free to try it (clone it and run it), feedback is welcome! Contact me on gitter (@Fristi) or open a issue.

## Setup

```scala
import flumina.types.ir._
import flumina.akkaimpl._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.duration._

implicit val actorSystem = ActorSystem()
implicit val materializer = ActorMaterializer()

val settings = KafkaSettings(
    bootstrapBrokers = Seq(KafkaBroker.Node("localhost", 9092)),
    connectionsPerBroker = 1,
    operationalSettings = KafkaOperationalSettings(
        retryBackoff = 500.milliseconds,
        retryMaxCount = 5,
        fetchMaxBytes = 32 * 1024,
        fetchMaxWaitTime = 300.milliseconds,
        produceTimeout = 1.seconds,
        groupSessionTimeout = 30.seconds
    ),
    requestTimeout = 30.seconds
)

val client = KafkaClient(settings)

val nrPartitions = 10
val topicName = "test"
```

## Producing values

```scala

//create a Map[TopicPartition, List[Record]], 
//all records grouped by their respective 
//topic and partition. 
val produce = (1 to 10000)
    .map(x => TopicPartition(topicName, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
    .toMultimap
    
//returns a Future[Result[Long]]
client.produce(produce)
```

## Consume topics

Streaming produce (ignoring the "produce" results) + streaming consumer (with stateful consumer groups).

```scala
val group = s"somegroup"
val topic1 = "topic1"
val topic2 = "topic2"
val size = 100000
val producer = client.producer(grouped = 5000, parallelism = 5)

Source(1 to size)
    .map(x => TopicPartition(topic1, 0) -> Record.fromByteValue(Seq(x.toByte)))
    .runWith(producer)

client.consume(groupId = s"${group}_a", topic = topic1, nrPartitions = 1)
    .map(x => x.record.value.head.toInt)
    .filter(_ % 2 == 0)
    .map(x => TopicPartition(topic2, 0) -> Record.fromByteValue(Seq(x.toByte)))
    .runWith(producer)

client.consume(groupId = s"${group}_b", topic = topic2, nrPartitions = 1)
    .runWith(Sink.foreach(println))
```