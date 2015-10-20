package monetdb

import java.nio.ByteBuffer

trait ValueConverter {
  def convert(value: java.lang.Object): Array[Byte]
}

class BooleanValueConverter extends ValueConverter {
  val bb: ByteBuffer = ByteBuffer.allocate(1);

  def convert(value: java.lang.Object): Array[Byte] = {
    bb.clear()
    val boolean: Boolean = value.asInstanceOf[java.lang.Boolean];

    if (boolean == null) {
      bb.put((-1).toByte);
    }
    else if (boolean == true) {
      bb.put(1.asInstanceOf[Byte]);
    }
    else if (boolean == false) {
      bb.put(0.asInstanceOf[Byte]);
    }
    //bb.put((Byte) value);
    return bb.array();
  }
}

class ByteValueConverter extends ValueConverter {
  val bb: ByteBuffer = ByteBuffer.allocate(1);

  def convert(value: java.lang.Object): Array[Byte] = {
    bb.clear();
    if (value == null) {
      bb.put(-1.asInstanceOf[Byte])// Byte.MIN_VALUE;
    }else {
      bb.put(value.asInstanceOf[Byte]);
    }
    return bb.array();
  }
}

class ShortValueConverter extends ValueConverter {
  val bb: ByteBuffer = ByteBuffer.allocate(java.lang.Short.BYTES)// 2

  def convert(value: java.lang.Object): Array[Byte] = {
    bb.clear();
    if (value == null) {
      bb.putShort(-1)
    }
    else {
      bb.putShort(value.asInstanceOf[Short]);
    }
    return bb.array();
  }
}

//implicit def stringWrapper(s: String) = {}
