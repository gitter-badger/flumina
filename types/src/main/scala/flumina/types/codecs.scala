package flumina.types

import scodec.bits.BitVector
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Encoder, SizeBound}

import scala.annotation.tailrec

private[types] class KafkaPartialVectorCodec[A](valueCodec: Codec[A]) extends Codec[Vector[A]] {

  override def encode(value: Vector[A]) = Encoder.encodeSeq(valueCodec)(value)

  override def decode(bits: BitVector): Attempt[DecodeResult[Vector[A]]] = {
    @tailrec
    def extract(acc: List[A], bitVector: BitVector): Attempt[DecodeResult[Vector[A]]] = {
      valueCodec.decode(bitVector) match {
        case Attempt.Successful(DecodeResult(value, remainder)) =>
          extract(value :: acc, remainder)
        case Attempt.Failure(err) =>
          Attempt.successful(DecodeResult(acc.reverse.toVector, BitVector.empty))
      }
    }

    extract(List.empty, bits)
  }

  override def sizeBound: SizeBound = SizeBound.unknown
}

private[types] class KafkaStringCodec extends Codec[Option[String]] {
  val codec = variableSizeBytes(int16, ascii)

  override def decode(bits: BitVector): Attempt[DecodeResult[Option[String]]] = for {
    size <- int16.decode(bits)
    str <- if (size.value == -1) Attempt.successful(DecodeResult(None, size.remainder))
    else variableSizeBytes(provide(size.value), ascii).decode(size.remainder).map(_.map(Some.apply))
  } yield str
  override def encode(value: Option[String]): Attempt[BitVector] = value match {
    case Some(str) => codec.encode(str)
    case None      => int16.encode(-1)
  }

  override def sizeBound: SizeBound = codec.sizeBound
}

private[types] class KafkaBytesCodec extends Codec[Vector[Byte]] {
  val codec = vectorOfN(int32, byte)

  override def decode(bits: BitVector): Attempt[DecodeResult[Vector[Byte]]] = for {
    size <- int32.decode(bits)
    xs <- if (size.value == -1) Attempt.successful(DecodeResult(Vector.empty[Byte], size.remainder))
    else vectorOfN(provide(size.value), byte).decode(size.remainder)
  } yield xs

  override def encode(value: Vector[Byte]): Attempt[BitVector] =
    if (value.isEmpty) int32.encode(-1)
    else codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound
}
