package vectos.kafka.types

import scodec._
import scodec.bits.ByteVector
import scodec.codecs._

sealed trait KafkaRequest

object KafkaRequest {

  final case class Produce(acks: Int, timeout: Int, topics: Vector[ProduceTopicRequest]) extends KafkaRequest
  final case class Fetch(replicaId: Int, maxWaitTime: Int, minBytes: Int, topics: Vector[FetchTopicRequest]) extends KafkaRequest
  final case class Metadata(topics: Vector[String]) extends KafkaRequest

  def produce(implicit topic: Codec[ProduceTopicRequest]): Codec[Produce] =
    (("acks" | int16) :: ("timeout" | int32) :: ("topics" | kafkaArray(topic))).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicRequest]): Codec[Fetch] =
    (("replicaId" | int32) :: ("maxWaitTime" | int32) :: ("minBytes" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]

  def metaData: Codec[Metadata] =
    ("topics" | kafkaArray(kafkaString)).as[Metadata]
}

case class RequestEnvelope(apiKey: Int, apiVersion: Int, correlationId: Int, clientId: String, request: ByteVector)

object RequestEnvelope {
  implicit val codec = (
    ("apiKey" | int16) ::
      ("apiVersion" | int16) ::
      ("correlationId" | int32) ::
      ("clientId" | kafkaString) ::
      ("request" | bytes)
    ).as[RequestEnvelope]
}
