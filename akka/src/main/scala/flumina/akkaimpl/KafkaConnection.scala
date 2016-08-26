package flumina.akkaimpl

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.io._
import akka.util.{ByteIterator, ByteString}
import scodec.Attempt
import flumina.types.{RequestEnvelope, ResponseEnvelope}

import scala.annotation.tailrec

/**
 * A connection to kafka, low level IO TCP akka implementation.
 * Handles folding and unfolding of envelopes, reconnection and encoding/decoding message size and buffering of incoming
 * messages.
 */
final class KafkaConnection private (pool: ActorRef, manager: ActorRef, broker: KafkaBroker.Node, retryStrategy: KafkaConnectionRetryStrategy) extends Actor with ActorLogging {

  import Tcp._
  import context.dispatcher

  private final case class Receiver(address: ActorRef, trace: Boolean)
  private final case class Ack(offset: Int) extends Tcp.Event

  private val maxStored = 100000000L
  private val highWatermark = maxStored * 5 / 10
  private val lowWatermark = maxStored * 3 / 10

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var readBuffer: ByteString = ByteString.empty
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var bytesToRead: Option[Int] = None
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var connection: ActorRef = _
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var inFlightRequests: Map[Int, Receiver] = Map()
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var nextCorrelationId: Int = Int.MinValue
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var writeBuffer: Vector[ByteString] = Vector()
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var storageOffset: Int = 0
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var stored: Int = 0
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var suspended: Boolean = false

  private def currentOffset = storageOffset + writeBuffer.size

  override def preStart() = connect()

  def reconnect(timesTriedToConnect: Int) = {
    retryStrategy.nextDelay(timesTriedToConnect) match {
      case Some(duration) =>
        log.info(s"Will retry to reconnect in: $duration")
        context.system.scheduler.scheduleOnce(duration, new Runnable {
          override def run(): Unit = connect()
        })

      case None =>
        log.error(s"Tried to reconnect $timesTriedToConnect a few times, but failed! shutting down")
        context stop self
    }
  }

  def connect() = {
    log.debug(s"Connecting to $broker")
    manager ! Tcp.Connect(new InetSocketAddress("localhost", broker.port))
  }

  def connecting(timesTriedToConnect: Int): Actor.Receive = {
    case CommandFailed(_) =>
      context.become(connecting(timesTriedToConnect + 1))
      reconnect(timesTriedToConnect + 1)

    case Connected(remote, local) =>
      connection = sender()
      pool ! KafkaConnectionPool.BrokerUp(self, broker)
      connection ! Register(self, keepOpenOnPeerClosed = true)
      context.become(connected)
  }

  def buffer(data: ByteString): Unit = {
    writeBuffer :+= data
    stored += data.size

    if (stored > maxStored) {
      log.error(s"drop connection due buffer overrun")
      context stop self

    } else if (stored > highWatermark) {
      log.debug(s"suspending reading at $currentOffset")
      connection ! SuspendReading
      suspended = true
    }
  }

  def acknowledge(ack: Int) = {
    require(ack === storageOffset, s"received ack $ack at $storageOffset")
    require(writeBuffer.nonEmpty, s"writeBuffer was empty at ack $ack")

    val size = writeBuffer(0).size
    stored -= size

    storageOffset += 1
    writeBuffer = writeBuffer drop 1

    if (suspended && stored < lowWatermark) {
      log.debug("resuming reading")
      connection ! ResumeReading
      suspended = false
    }
  }

  def read(data: ByteString) = {
    def decode(buffer: ByteString) = {
      decodeEnvelope(buffer) match {
        case Attempt.Successful(env) =>
          inFlightRequests.get(env.correlationId) match {
            case Some(receiver) =>
              if (receiver.trace) {
                log.debug(s"Response for ${env.correlationId} -> ${env.response.toHex}")
              }
              receiver.address ! Status.Success(env.response)
            case None => log.warning(s"There was no receiver for ${env.correlationId}")
          }

          inFlightRequests -= env.correlationId

        case Attempt.Failure(err) =>
          log.error(s"Error decoding: ${err.messageWithContext}")
      }
    }

    bytesToRead match {
      case Some(toRead) =>
        val newBuffer = readBuffer ++ data

        if (newBuffer.size >= toRead) {
          decode(newBuffer.take(toRead))
          readBuffer = newBuffer.drop(toRead)
          bytesToRead = None

        } else {
          readBuffer = newBuffer
        }

      case None if data.size < 4 =>
        readBuffer ++= data
      case None =>
        val toRead = bigEndianDecoder(data.take(4).iterator, 4)
        val newBuffer = data.drop(4)

        if (newBuffer.size >= toRead) {
          decode(newBuffer.take(toRead))
          readBuffer = newBuffer.drop(toRead)
        } else {
          readBuffer = newBuffer
          bytesToRead = Some(toRead)
        }
    }
  }

  private def writeFirst(): Unit = {
    connection ! Write(writeBuffer(0), Ack(storageOffset))
  }

  private def writeAll(): Unit = {
    for ((data, i) <- writeBuffer.zipWithIndex) {
      connection ! Write(data, Ack(storageOffset + i))
    }
  }

  def goReconnect() = {
    //TODO: reset all state?
    pool ! KafkaConnectionPool.BrokerDown(self, broker)
    context.become(connecting(0))
    reconnect(0)
  }

  def connected: Actor.Receive = {
    case request: KafkaConnectionRequest =>
      encodeEnvelope(nextCorrelationId, request) match {
        case Attempt.Successful(msg) =>
          connection ! Write(msg, Ack(currentOffset))
          buffer(msg)
          inFlightRequests += (nextCorrelationId -> Receiver(sender(), request.trace))
          nextCorrelationId += 1

        case Attempt.Failure(err) => log.error(s"Error encoding: ${err.messageWithContext}")
      }
    case Ack(ack) => acknowledge(ack)
    case CommandFailed(Write(_, Ack(ack))) =>
      connection ! ResumeWriting
      context become buffering(nack = ack, toAck = 10, peerClosed = false)

    case Received(data) => read(data)
    case PeerClosed =>
      if (writeBuffer.isEmpty) context stop self
      else context become closing

    case Aborted => goReconnect()
    case Closed  => goReconnect()

  }

  def buffering(nack: Int, toAck: Int, peerClosed: Boolean): Actor.Receive = {
    case request: KafkaConnectionRequest =>
      encodeEnvelope(nextCorrelationId, request) match {
        case Attempt.Successful(msg) =>
          buffer(msg)
          inFlightRequests += (nextCorrelationId -> Receiver(sender(), request.trace))
          nextCorrelationId += 1

        case Attempt.Failure(err) => log.error(s"Error encoding: ${err.messageWithContext}")
      }

    case WritingResumed         => writeFirst()
    case Received(data)         => read(data)
    case PeerClosed             => context.become(buffering(nack, toAck, peerClosed = true))
    case Aborted                => goReconnect()
    case Closed                 => goReconnect()

    case Ack(ack) if ack < nack => acknowledge(ack)
    case Ack(ack) =>
      acknowledge(ack)
      if (writeBuffer.nonEmpty) {
        if (toAck > 0) {
          // stay in ACK-based mode for a while
          writeFirst()
          context.become(buffering(nack, toAck - 1, peerClosed))
        } else {
          // then return to NACK-based again
          writeAll()
          context become (if (peerClosed) closing else connected)
        }
      } else if (peerClosed) context stop self
      else context become connected
  }

  def closing: Actor.Receive = {
    case CommandFailed(_: Write) =>
      connection ! ResumeWriting
      context.become({

        case WritingResumed =>
          writeAll()
          context.unbecome()

        case ack: Int => acknowledge(ack)

      }, discardOld = false)

    case Ack(ack) =>
      acknowledge(ack)
      if (writeBuffer.isEmpty) context stop self
  }

  def receive = connecting(timesTriedToConnect = 0)

  private val bigEndianDecoder: (ByteIterator, Int) ⇒ Int = (bs, length) ⇒ {
    @tailrec
    def run(count: Int, decoded: Int): Int = {
      if (count > 0) {
        run(count - 1, decoded << 8 | bs.next().toInt & 0xFF)
      } else {
        decoded
      }
    }

    run(length, 0)
  }

  def encodeEnvelope(correlationId: Int, request: KafkaConnectionRequest) = {
    RequestEnvelope.codec.encode(RequestEnvelope(request.apiKey, request.version, correlationId, Some("flumina"), request.requestPayload)).map { bytes =>

      val msg = bytes.toByteVector.toByteString
      val msgSize = msg.size
      val header = ByteString((msgSize >> 24) & 0xFF, (msgSize >> 16) & 0xFF, (msgSize >> 8) & 0xFF, msgSize & 0xFF)

      if (request.trace) {
        log.debug(s"Request $correlationId encoded ($msgSize) -> ${bytes.toHex}")
      }

      header ++ msg
    }
  }

  def decodeEnvelope(byteString: ByteString) = ResponseEnvelope.codec.decodeValue(byteString.toByteVector.toBitVector)
}

object KafkaConnection {
  def props(pool: ActorRef, manager: ActorRef, broker: KafkaBroker.Node, retryStrategy: KafkaConnectionRetryStrategy) = Props(new KafkaConnection(pool, manager, broker, retryStrategy))
}
