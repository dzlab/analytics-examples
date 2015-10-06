package common

import java.nio.ByteBuffer
import java.text.SimpleDateFormat

sealed trait Converter {
  def convert(value: String): java.lang.Object
  def toBytes(value: String): ByteBuffer
  def zero: java.lang.Object
  def zeroAsBytes: ByteBuffer// = toBytes("0")
}

object ByteUtil {
  val EMPTY_BYTE_BUFFER: ByteBuffer = ByteBuffer.wrap(Array.fill(0)(0.asInstanceOf[Byte]))//Array[Byte](0))

}

object int extends Converter {
  def convert(value: String): java.lang.Integer = java.lang.Integer.valueOf(value)
  def toBytes(value: String): ByteBuffer = {
    val bb = ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(convert(value))//ByteBuffer.wrap(Array.fill(4)(0.asInstanceOf[Byte])).putInt(convert(value))
    bb.position(0)
    bb
  }
  val zero: java.lang.Integer = java.lang.Integer.valueOf("0")
  val zeroAsBytes: ByteBuffer = toBytes("0")
}

object bigint extends Converter {
  def convert(value: String): java.lang.Long = java.lang.Long.valueOf(value)
  def toBytes(value: String): ByteBuffer = {
    val bb = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(convert(value))//ByteBuffer.wrap(Array.fill(8)(0.asInstanceOf[Byte])).putLong(convert(value))
    bb.position(0)
    bb
  }
  val zero: java.lang.Long = java.lang.Long.valueOf("0")
  val zeroAsBytes: ByteBuffer = toBytes("0")
}

object bool extends Converter {
  def convert(value: String): java.lang.Boolean = java.lang.Boolean.valueOf(value)
  def toBytes(value: String): ByteBuffer = {
    val bb = if (value==null) ByteUtil.EMPTY_BYTE_BUFFER
              else if(convert(value)) ByteBuffer.wrap(Array[Byte](1))  // true
              else ByteBuffer.wrap(Array[Byte](0)); // false
    bb.position(0)
    bb
  }   
  val zero: java.lang.Boolean = java.lang.Boolean.valueOf("false")
  val zeroAsBytes: ByteBuffer = toBytes("false")
}

object double extends Converter {
  def convert(value: String): java.lang.Double = if(value=="NULL") null else java.lang.Double.valueOf(value)
  def toBytes(value: String): ByteBuffer = {
    val bb = ByteBuffer.allocate(java.lang.Double.BYTES).putDouble(convert(value))//ByteBuffer.wrap(Array[Byte](8)).putDouble(convert(value))
    bb.position(0)
    bb
  }
  val zero: java.lang.Double = java.lang.Double.valueOf("0.0")
  val zeroAsBytes: ByteBuffer = toBytes("0.0")
}

object timestamp extends Converter {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")// 2015-08-12 11:34:28
  def convert(value: String): java.util.Date = {
    require(value!=null, "Cannot convert null to Date")
    formatter.parse(value)
  }
  def toBytes(value: String): ByteBuffer = {
    val bb = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(convert(value).getTime)
    bb.position(0)
    bb
  }
  val zero: java.util.Date = new java.util.Date(0)
  def zeroAsBytes: ByteBuffer = toBytes("0")
}

object text extends Converter {
  def convert(value: String): java.lang.String = value
  def toBytes(value: String): ByteBuffer = {
    val bb = ByteBuffer.wrap(value.getBytes)
    bb.position(0)
    bb
  }
  val zero: java.lang.String = new java.lang.String("")
  val zeroAsBytes: ByteBuffer = toBytes("")
}
