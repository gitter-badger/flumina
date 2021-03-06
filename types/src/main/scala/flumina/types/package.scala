package flumina

import scodec.Codec
import scodec.codecs._

package object types {

  val kafkaString: Codec[Option[String]] = new KafkaStringCodec
  val kafkaBytes: Codec[Vector[Byte]] = new KafkaBytesCodec

  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] = vectorOfN(int32, valueCodec)
  def partialVector[A](valueCodec: Codec[A]): Codec[Vector[A]] = new KafkaPartialVectorCodec[A](valueCodec)
}
