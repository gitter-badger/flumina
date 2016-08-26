package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import cats.scalatest.{XorMatchers, XorValues}
import flumina.types.ir.{Record, RecordEntry, TopicPartition}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

abstract class KafkaClientTest extends TestKit(ActorSystem())
    with WordSpecLike
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures
    with KafkaDockerTest
    with Matchers
    with XorMatchers
    with XorValues
    with Inspectors
    with OptionValues {

  import system.dispatcher

  "KafkaClient" should {

    "produce and fetch (from/to) multiple partitions" in new KafkaScope {
      val nrPartitions = 10
      val name = randomTopic(partitions = nrPartitions, replicationFactor = 1)
      val size = 5000
      val produce = (1 to size)
        .map(x => TopicPartition(name, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
        .toMultimap

      val prg = for {
        produceResult <- client.produce(produce)
        _ <- FutureUtils.delay(1.second)
        fetchResult <- client.singleFetch(TopicPartition.enumerate(name, nrPartitions).map(_ -> 0l).toMap)
      } yield produceResult -> fetchResult

      whenReady(prg) {
        case (produceResult, fetchResult) =>
          //check if the produceResult has error
          produceResult.errors should have size 0
          produceResult.success should have size nrPartitions.toLong

          //check if the fetchResult has error
          fetchResult.errors should have size 0
          fetchResult.success should have size nrPartitions.toLong
          //it should be evenly divided
          forAll(fetchResult.success) { y => y.value.size shouldBe (size / nrPartitions).toLong }
      }
    }

    "staged produce and consume" in new KafkaScope {
      val group = s"test${System.currentTimeMillis()}"
      val topic1 = randomTopic(partitions = 1, replicationFactor = 1)
      val topic2 = randomTopic(partitions = 1, replicationFactor = 1)
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
        .runWith(TestSink.probe[RecordEntry])
        .ensureSubscription()
        .request(50000)
        .expectNextN(50000) should have size 50000
    }
  }

  final def kafkaScaling = 3

  private def kafka1Port: Int = KafkaDocker.getPortFor("kafka", 1).getOrElse(sys.error("Unable to get port for kafka 1"))
  private def zookeeperPort: Int = 2181

  private def deadServer(nr: Int) = KafkaBroker.Node(s"localhost", 12300 + nr)

  private lazy val settings = KafkaSettings(
    bootstrapBrokers = Seq(KafkaBroker.Node("localhost", kafka1Port)),
    //    bootstrapBrokers = Seq(deadServer(1), deadServer(2), KafkaBroker.Node("localhost", kafka1Port)),
    connectionsPerBroker = 3,
    operationalSettings = KafkaOperationalSettings(
      retryBackoff = 500.milliseconds,
      retryMaxCount = 5,
      fetchMaxBytes = 32 * 1024,
      fetchMaxWaitTime = 1.seconds,
      produceTimeout = 1.seconds,
      groupSessionTimeout = 30.seconds,
      heartbeatFrequency = 4
    ),
    requestTimeout = 30.seconds
  )

  private lazy val client = KafkaClient(settings)

  trait KafkaScope {
    def randomTopic(partitions: Int, replicationFactor: Int) = {
      val name = s"test${System.nanoTime().toString}"
      Utils.createTopic(name, partitions, replicationFactor, zookeeperPort)
      Thread.sleep(1000)
      name
    }
  }

  private implicit val actorMaterializer = ActorMaterializer()(system)

  override implicit def patienceConfig = PatienceConfig(timeout = 30.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
