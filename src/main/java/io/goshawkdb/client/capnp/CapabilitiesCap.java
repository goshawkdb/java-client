// Generated by Cap'n Proto compiler, DO NOT EDIT
// source: capabilities.capnp

package io.goshawkdb.client.capnp;

public final class CapabilitiesCap {
  public static class Capability {
    public static final org.capnproto.StructSize STRUCT_SIZE = new org.capnproto.StructSize((short)1,(short)0);
    public static final class Factory extends org.capnproto.StructFactory<Builder, Reader> {
      public Factory() {
      }
      public final Reader constructReader(org.capnproto.SegmentReader segment, int data,int pointers, int dataSize, short pointerCount, int nestingLimit) {
        return new Reader(segment,data,pointers,dataSize,pointerCount,nestingLimit);
      }
      public final Builder constructBuilder(org.capnproto.SegmentBuilder segment, int data,int pointers, int dataSize, short pointerCount) {
        return new Builder(segment, data, pointers, dataSize, pointerCount);
      }
      public final org.capnproto.StructSize structSize() {
        return Capability.STRUCT_SIZE;
      }
      public final Reader asReader(Builder builder) {
        return builder.asReader();
      }
    }
    public static final Factory factory = new Factory();
    public static final org.capnproto.StructList.Factory<Builder,Reader> listFactory =
      new org.capnproto.StructList.Factory<Builder, Reader>(factory);
    public static final class Builder extends org.capnproto.StructBuilder {
      Builder(org.capnproto.SegmentBuilder segment, int data, int pointers,int dataSize, short pointerCount){
        super(segment, data, pointers, dataSize, pointerCount);
      }
      public Which which() {
        switch(_getShortField(0)) {
          case 0 : return Which.NONE;
          case 1 : return Which.READ;
          case 2 : return Which.WRITE;
          case 3 : return Which.READ_WRITE;
          default: return Which._NOT_IN_SCHEMA;
        }
      }
      public final Reader asReader() {
        return new Reader(segment, data, pointers, dataSize, pointerCount, 0x7fffffff);
      }
      public final boolean isNone() {
        return which() == Capability.Which.NONE;
      }
      public final org.capnproto.Void getNone() {
        assert which() == Capability.Which.NONE:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }
      public final void setNone(org.capnproto.Void value) {
        _setShortField(0, (short)Capability.Which.NONE.ordinal());
      }

      public final boolean isRead() {
        return which() == Capability.Which.READ;
      }
      public final org.capnproto.Void getRead() {
        assert which() == Capability.Which.READ:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }
      public final void setRead(org.capnproto.Void value) {
        _setShortField(0, (short)Capability.Which.READ.ordinal());
      }

      public final boolean isWrite() {
        return which() == Capability.Which.WRITE;
      }
      public final org.capnproto.Void getWrite() {
        assert which() == Capability.Which.WRITE:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }
      public final void setWrite(org.capnproto.Void value) {
        _setShortField(0, (short)Capability.Which.WRITE.ordinal());
      }

      public final boolean isReadWrite() {
        return which() == Capability.Which.READ_WRITE;
      }
      public final org.capnproto.Void getReadWrite() {
        assert which() == Capability.Which.READ_WRITE:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }
      public final void setReadWrite(org.capnproto.Void value) {
        _setShortField(0, (short)Capability.Which.READ_WRITE.ordinal());
      }

    }

    public static final class Reader extends org.capnproto.StructReader {
      Reader(org.capnproto.SegmentReader segment, int data, int pointers,int dataSize, short pointerCount, int nestingLimit){
        super(segment, data, pointers, dataSize, pointerCount, nestingLimit);
      }

      public Which which() {
        switch(_getShortField(0)) {
          case 0 : return Which.NONE;
          case 1 : return Which.READ;
          case 2 : return Which.WRITE;
          case 3 : return Which.READ_WRITE;
          default: return Which._NOT_IN_SCHEMA;
        }
      }
      public final boolean isNone() {
        return which() == Capability.Which.NONE;
      }
      public final org.capnproto.Void getNone() {
        assert which() == Capability.Which.NONE:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }

      public final boolean isRead() {
        return which() == Capability.Which.READ;
      }
      public final org.capnproto.Void getRead() {
        assert which() == Capability.Which.READ:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }

      public final boolean isWrite() {
        return which() == Capability.Which.WRITE;
      }
      public final org.capnproto.Void getWrite() {
        assert which() == Capability.Which.WRITE:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }

      public final boolean isReadWrite() {
        return which() == Capability.Which.READ_WRITE;
      }
      public final org.capnproto.Void getReadWrite() {
        assert which() == Capability.Which.READ_WRITE:
                    "Must check which() before get()ing a union member.";
        return org.capnproto.Void.VOID;
      }

    }

    public enum Which {
      NONE,
      READ,
      WRITE,
      READ_WRITE,
      _NOT_IN_SCHEMA,
    }
  }



public static final class Schemas {
public static final org.capnproto.SegmentReader b_a2976928591c7a84 =
   org.capnproto.GeneratedClassSupport.decodeRawBytes(
   "\u0000\u0000\u0000\u0000\u0005\u0000\u0006\u0000" +
   "\u0084\u007a\u001c\u0059\u0028\u0069\u0097\u00a2" +
   "\u0013\u0000\u0000\u0000\u0001\u0000\u0001\u0000" +
   "\u008c\u006f\u0065\u0009\u002f\u00fc\u00ba\u00d9" +
   "\u0000\u0000\u0007\u0000\u0000\u0000\u0004\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0015\u0000\u0000\u0000\u00f2\u0000\u0000\u0000" +
   "\u0021\u0000\u0000\u0000\u0007\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u001d\u0000\u0000\u0000\u00e7\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0063\u0061\u0070\u0061\u0062\u0069\u006c\u0069" +
   "\u0074\u0069\u0065\u0073\u002e\u0063\u0061\u0070" +
   "\u006e\u0070\u003a\u0043\u0061\u0070\u0061\u0062" +
   "\u0069\u006c\u0069\u0074\u0079\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0001\u0000\u0001\u0000" +
   "\u0010\u0000\u0000\u0000\u0003\u0000\u0004\u0000" +
   "\u0000\u0000\u00ff\u00ff\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0001\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0061\u0000\u0000\u0000\u002a\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\\\u0000\u0000\u0000\u0003\u0000\u0001\u0000" +
   "\u0068\u0000\u0000\u0000\u0002\u0000\u0001\u0000" +
   "\u0001\u0000\u00fe\u00ff\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0001\u0000\u0001\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0065\u0000\u0000\u0000\u002a\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0060\u0000\u0000\u0000\u0003\u0000\u0001\u0000" +
   "\u006c\u0000\u0000\u0000\u0002\u0000\u0001\u0000" +
   "\u0002\u0000\u00fd\u00ff\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0001\u0000\u0002\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0069\u0000\u0000\u0000\u0032\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0064\u0000\u0000\u0000\u0003\u0000\u0001\u0000" +
   "\u0070\u0000\u0000\u0000\u0002\u0000\u0001\u0000" +
   "\u0003\u0000\u00fc\u00ff\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0001\u0000\u0003\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u006d\u0000\u0000\u0000\u0052\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u006c\u0000\u0000\u0000\u0003\u0000\u0001\u0000" +
   "\u0078\u0000\u0000\u0000\u0002\u0000\u0001\u0000" +
   "\u006e\u006f\u006e\u0065\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0072\u0065\u0061\u0064\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0077\u0072\u0069\u0074\u0065\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0072\u0065\u0061\u0064\u0057\u0072\u0069\u0074" +
   "\u0065\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" +
   "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000" + "");
}
}

