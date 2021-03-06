package flumina.types.v0

import scodec._
import scodec.codecs._
import flumina.types._

sealed trait KafkaRequest

object KafkaRequest {

  final case class Produce(acks: Int, timeout: Int, topics: Vector[ProduceTopicRequest]) extends KafkaRequest
  final case class Fetch(replicaId: Int, maxWaitTime: Int, minBytes: Int, topics: Vector[FetchTopicRequest]) extends KafkaRequest
  final case class ListOffset(replicaId: Int, topics: Vector[ListOffsetTopicRequest]) extends KafkaRequest
  final case class Metadata(topics: Vector[Option[String]]) extends KafkaRequest
  final case class OffsetCommit(groupId: Option[String], topics: Vector[OffsetCommitTopicRequest]) extends KafkaRequest
  final case class OffsetFetch(groupId: Option[String], topics: Vector[OffsetFetchTopicRequest]) extends KafkaRequest
  final case class GroupCoordinator(groupId: Option[String]) extends KafkaRequest
  final case class JoinGroup(groupId: Option[String], sessionTimeOut: Int, memberId: Option[String], protocolType: Option[String], groupProtocols: Vector[JoinGroupProtocolRequest]) extends KafkaRequest
  final case class Heartbeat(groupId: Option[String], generationId: Int, memberId: Option[String]) extends KafkaRequest
  final case class LeaveGroup(groupId: Option[String], memberId: Option[String]) extends KafkaRequest
  final case object ListGroups extends KafkaRequest
  final case class DescribeGroups(groupIds: Vector[Option[String]]) extends KafkaRequest
  final case class SyncGroup(groupId: Option[String], generationId: Int, memberId: Option[String], groupAssignment: Vector[SyncGroupGroupAssignmentRequest]) extends KafkaRequest

  def produce(implicit topic: Codec[ProduceTopicRequest]): Codec[Produce] =
    (("acks" | int16) :: ("timeout" | int32) :: ("topics" | kafkaArray(topic))).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicRequest]): Codec[Fetch] =
    (("replicaId" | int32) :: ("maxWaitTime" | int32) :: ("minBytes" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]

  def listOffset(implicit topic: Codec[ListOffsetTopicRequest]): Codec[ListOffset] =
    (("replicaId" | int32) :: ("topics" | kafkaArray(topic))).as[ListOffset]

  def metaData: Codec[Metadata] =
    ("topics" | kafkaArray(kafkaString)).as[Metadata]

  def offsetCommit(implicit topic: Codec[OffsetCommitTopicRequest]): Codec[OffsetCommit] =
    (("groupId" | kafkaString) :: ("topics" | kafkaArray(topic))).as[OffsetCommit]

  def offsetFetch(implicit topic: Codec[OffsetFetchTopicRequest]): Codec[OffsetFetch] =
    (("groupId" | kafkaString) :: ("topics" | kafkaArray(topic))).as[OffsetFetch]

  def groupCoordinator: Codec[GroupCoordinator] =
    ("groupId" | kafkaString).as[GroupCoordinator]

  def joinGroup(implicit groupProtocol: Codec[JoinGroupProtocolRequest]): Codec[JoinGroup] =
    (
      ("groupId" | kafkaString) ::
      ("sessionTimeOut" | int32) ::
      ("memberId" | kafkaString) ::
      ("protocolType" | kafkaString) ::
      ("groupProtocols" | kafkaArray(groupProtocol))
    ).as[JoinGroup]

  def heartbeat: Codec[Heartbeat] =
    (("groupId" | kafkaString) :: ("generationId" | int32) :: ("memberId" | kafkaString)).as[Heartbeat]

  def leaveGroup: Codec[LeaveGroup] =
    (("groupId" | kafkaString) :: ("memberId" | kafkaString)).as[LeaveGroup]

  def listGroups: Codec[ListGroups.type] =
    provide(ListGroups).as[ListGroups.type]

  def describeGroups: Codec[DescribeGroups] =
    ("groups" | kafkaArray(kafkaString)).as[DescribeGroups]

  def syncGroup(implicit assignment: Codec[SyncGroupGroupAssignmentRequest]): Codec[SyncGroup] =
    (
      ("groupId" | kafkaString) ::
      ("generationId" | int32) ::
      ("memberId" | kafkaString) ::
      ("groupAssignment" | kafkaArray(assignment))
    ).as[SyncGroup]

}

