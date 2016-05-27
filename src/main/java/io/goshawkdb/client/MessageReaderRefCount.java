package io.goshawkdb.client;

import org.capnproto.MessageReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

class MessageReaderRefCount implements ByteBufHolder {

    final ByteBuf buf;

    final MessageReader msg;

    MessageReaderRefCount(final ByteBuf b, final MessageReader msgReader) {
        buf = b;
        msg = msgReader;
        buf.retain();
    }

    @Override
    public ByteBuf content() {
        return buf;
    }

    @Override
    public ByteBufHolder copy() {
        return new MessageReaderRefCount(buf.copy(), msg);
    }

    @Override
    public ByteBufHolder duplicate() {
        return new MessageReaderRefCount(buf.duplicate(), msg);
    }

    @Override
    public ByteBufHolder retainedDuplicate() {
        return new MessageReaderRefCount(buf.retainedDuplicate(), msg);
    }

    @Override
    public ByteBufHolder replace(final ByteBuf content) {
        return new MessageReaderRefCount(content, msg);
    }

    @Override
    public int refCnt() {
        return buf.refCnt();
    }

    @Override
    public ByteBufHolder retain() {
        buf.retain();
        return this;
    }

    @Override
    public ByteBufHolder retain(final int increment) {
        buf.retain(increment);
        return this;
    }

    @Override
    public ByteBufHolder touch() {
        buf.touch();
        return this;
    }

    @Override
    public ByteBufHolder touch(Object hint) {
        buf.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return buf.release();
    }

    @Override
    public boolean release(final int decrement) {
        return buf.release(decrement);
    }
}
