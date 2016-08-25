package flumina.akkaimpl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import cats.data.Xor
import flumina.types.ir._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

final class KafkaClient private (settings: KafkaSettings, actorSystem: ActorSystem) {

  private implicit val timeout: Timeout = Timeout(settings.requestTimeout)
  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  private val coordinator = actorSystem.actorOf(KafkaCoordinator.props(settings))

  def produce(values: Map[TopicPartition, List[Record]]) = (coordinator ? Produce(values)).mapTo[Result[Long]]

  def producer(grouped: Int) =
    Flow[(TopicPartition, Record)]
      .grouped(grouped)
      .mapAsync(1)(x => produce(x.toMultimap))
      .to(Sink.ignore)

  private def splitEvenly[A](sequence: TraversableOnce[A], over: Int) = {
    @tailrec
    def go(seq: List[A], acc: List[List[A]], split: Int): List[List[A]] =
      if (seq.size <= split) seq :: acc
      else go(seq.drop(split), seq.take(split) :: acc, split)

    go(sequence.toList, List.empty, sequence.size / over)
  }

  def consume(groupId: String, topic: String, nrPartitions: Int): Source[RecordEntry, NotUsed] = {
    def init = for {
      joinGroupResult <- joinGroup(groupId, "consumer", Seq(GroupProtocol("range", Seq(ConsumerProtocol(0, Seq(topic), Seq.empty)))))
      memberAssignment <- joinGroupResult match {
        case Xor.Left(kafkaResult) =>
          Future.failed(new Exception("Failed to join group...?"))
        case Xor.Right(groupResult) =>
          if (groupResult.leaderId == groupResult.memberId) {
            val topicPartitionChunks = splitEvenly(TopicPartition.enumerate(topic, nrPartitions), groupResult.members.size)
            val assignments = groupResult.members
              .zipWithIndex
              .map { case (m, idx) => GroupAssignment(m.memberId, MemberAssignment(0, topicPartitionChunks(idx), "assignment for member".getBytes())) }

            syncGroup(groupId, groupResult.generationId, groupResult.memberId, assignments)
              .flatMap(_.toFuture(err => new Exception(s"Error occurred: $err")))
          } else {
            syncGroup(groupId, groupResult.generationId, groupResult.memberId, Seq.empty)
              .flatMap(_.toFuture(err => new Exception(s"Error occurred: $err")))
          }
      }
      //      offsets <- offsetsFetch(groupId, memberAssignment.topicPartitions.toSet)
      //      _ = println(s"offsets: $offsets")

      //      offsetMap = offsets.success.map(x => x.topicPartition -> x.value.offset).toMap
      offsetMap = TopicPartition.enumerate(topic, nrPartitions).map(x => x -> 0l).toMap
      fetch <- singleFetch(offsetMap)
    } yield (joinGroupResult, fetch, offsetMap)

    def run(joinGroupResult: JoinGroupResult, nextDelayInSeconds: Int, lastResult: Result[List[RecordEntry]], lastOffsetRequest: Map[TopicPartition, Long]): Source[RecordEntry, NotUsed] = {

      println(s"lastOffsetRequest: $lastOffsetRequest")
      println(s"lastResult [errors: ${lastResult.errors.nonEmpty}, " +
        s"offsets: ${lastResult.success.flatMap(x => x.value.map(x.topicPartition -> _.offset))}]")
//      println(s"got back: ${lastResult.success.flatMap(_.value).take(10)} results (${lastResult.errors.size})")

      if (lastResult.errors.nonEmpty) {
        Source.failed(new Exception("Failing..."))
      } else {
        val newOffsetRequest = if(lastResult.success.nonEmpty) {
          lastResult.success
            .map(x => x.topicPartition -> (if (x.value.isEmpty) 0l else x.value.maxBy(y => y.offset).offset))
            .toMap
        } else {
          lastOffsetRequest
        }

        println(s"newOffsetRequest === lastOffsetRequest: ${newOffsetRequest === lastOffsetRequest}")

        if (newOffsetRequest === lastOffsetRequest) {
          def next = Source.fromFuture {
            for {
              //TODO: check output
//              _ <- heartbeat(groupId, joinGroupResult.generationId, joinGroupResult.memberId)
              _ <- FutureUtils.delay(1.seconds * Math.min(30, nextDelayInSeconds).toLong)
              newResults <- singleFetch(newOffsetRequest)
            } yield newResults
          }

          next.flatMapConcat(r => run(joinGroupResult, nextDelayInSeconds + 1, r, newOffsetRequest))
        } else {
          def next = Source.fromFuture {
            for {
              //TODO: check output
//              _ <- heartbeat(groupId, joinGroupResult.generationId, joinGroupResult.memberId)
              //TODO: check output
              _ <- offsetsCommit(groupId, lastOffsetRequest.mapValues(x => OffsetMetadata(x, None)))
              newResults <- singleFetch(newOffsetRequest)
            } yield newResults
          }

          Source(lastResult.success.flatMap(_.value)) ++ next.flatMapConcat(r => run(joinGroupResult, 0, r, newOffsetRequest))
        }
      }
    }

    Source.fromFuture(init).flatMapConcat {
      case (joinGroupResult, firstFetch, offsets) =>
        joinGroupResult match {
          case Xor.Left(err)  => Source.failed(new Exception(s"Error occurred: $err"))
          case Xor.Right(res) => run(res, 0, firstFetch, offsets)
        }
    }
  }

  private def offsetsFetch(groupId: String, values: Set[TopicPartition]) =
    (coordinator ? OffsetsFetch(groupId, values)).mapTo[Result[OffsetMetadata]]

  private def offsetsCommit(groupId: String, offsets: Map[TopicPartition, OffsetMetadata]) =
    (coordinator ? OffsetsCommit(groupId, offsets)).mapTo[Result[Unit]]

  def singleFetch(values: Map[TopicPartition, Long]) =
    (coordinator ? Fetch(values)).mapTo[Result[List[RecordEntry]]]

  private def joinGroup(groupId: String, protocol: String, protocols: Seq[GroupProtocol]) =
    (coordinator ? JoinGroup(groupId, protocol, protocols)).mapTo[KafkaError Xor JoinGroupResult]

  private def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) =
    (coordinator ? SynchronizeGroup(groupId, generationId, memberId, assignments)).mapTo[KafkaError Xor MemberAssignment]

  private def heartbeat(groupId: String, generationId: Int, memberId: String) =
    (coordinator ? Heartbeat(groupId, generationId, memberId)).mapTo[KafkaError Xor Unit]
}

object KafkaClient {
  def apply(settings: KafkaSettings)(implicit system: ActorSystem) =
    new KafkaClient(settings, system)
}