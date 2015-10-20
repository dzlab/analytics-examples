package common;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;

public class Converters {

    public static interface ValueConverter {
        byte[] convert(Object value);
    }

    public static class BooleanValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(1);

        public byte[] convert(Object value) {
            bb.clear();
            Boolean val = (Boolean) value;

            if (val == null) {
                bb.put((byte) Byte.MIN_VALUE);
            }
            if (val == true) {
                bb.put((byte) 1);
            }
            if (val == false) {
                bb.put((byte) 0);
            }
            bb.put((Byte) value);
            return bb.array();
        }
    }

    public static class ByteValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(1);

        public byte[] convert(Object value) {
            bb.clear();
            if (value == null) {
                value = Byte.MIN_VALUE;
            }
            bb.put((Byte) value);
            return bb.array();
        }
    }

    public static class ShortValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(2);

        public byte[] convert(Object value) {
            bb.clear();
            if (value == null) {
                value = Byte.MIN_VALUE;
            }
            bb.putShort((Short) value);
            return bb.array();
        }
    }

    public static class IntegerValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(4);

        public byte[] convert(Object value) {
            bb.clear();
            if (value == null) {
                value = Integer.MIN_VALUE;
            }
            bb.putInt((Integer) value);
            return bb.array();
        }
    }

    public static class LongValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(8);

        public byte[] convert(Object value) {
            bb.clear();
            if (value == null) {
                value = Long.MIN_VALUE;
            }
            bb.putLong((Long) value);
            return bb.array();
        }
    }

    public static class FloatValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(4);

        public byte[] convert(Object value) {
            bb.clear();
            if (value == null) {
                value = Float.MIN_VALUE;
            }
            bb.putFloat((Float) value);
            return bb.array();
        }
    }

    public static class DoubleValueConverter implements ValueConverter {
        private ByteBuffer bb = ByteBuffer.allocate(8);

        public byte[] convert(Object value) {
            bb.clear();
            if (value == null) {
                value = Double.MIN_VALUE;
            }
            bb.putDouble((Double) value);
            return bb.array();
        }
    }

    public static class StringValueConverter implements ValueConverter {
        public byte[] convert(Object value) {
            return (((String) value) + "\n").getBytes();
        }
    }

    public static class TimestampValueConverter implements ValueConverter {
	private ByteBuffer bb = ByteBuffer.allocate(java.lang.Long.BYTES);

	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public byte[] convert(Object value) {
	    bb.clear();
	    if(value instanceof String) {
	      try {
		bb.putLong(formatter.parse((String) value).getTime());
	      } catch(Exception e) {
                e.printStackTrace();
              }
            }else if(value instanceof Long) {
	      bb.putLong((Long) value);
	    }
	    return bb.array();
	}
    }
}
