package io.goshawkdb.client;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.ReaderOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

final class CapnProtoCodec extends ByteToMessageCodec<MessageBuilder> {
    private static final int MAX_SEGMENT_NUMBER = 1024;
    private static final int MAX_TOTAL_SIZE = 1024 * 1024 * 1024;

    private final Connection conn;

    CapnProtoCodec(final Connection connection) {
        conn = connection;
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        conn.disconnected();
        super.channelInactive(ctx);
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final MessageBuilder msg, final ByteBuf out) throws Exception {
        final ByteBuffer[] segments = msg.getSegmentsForOutput();
        out.writeIntLE(segments.length - 1);

        for (ByteBuffer seg : segments) {
            out.writeIntLE(seg.limit() / 8);
        }

        // We've written 4 bytes for the segCount, then segCount*4 bytes for each len. But the
        // segments themselves must start on the next 8-byte boundary. So if we segCount is even
        // then we need to add 4 bytes of padding here.
        if (segments.length % 2 == 0) {
            out.writeIntLE(0);
        }

        for (ByteBuffer seg : segments) {
            out.writeBytes(seg);
        }
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
        int available = in.readableBytes();
        if (available < 4) {
            return;
        }
        in.markReaderIndex();

        final int segCount = in.readIntLE() + 1;
        available -= 4;
        if (1 > segCount || segCount > MAX_SEGMENT_NUMBER) {
            in.resetReaderIndex();
            throw new IOException("Too many segments: " + segCount);
        }
        final int headerSize = 4 * segCount;
        if (available < headerSize) {
            in.resetReaderIndex();
            return;
        }
        final int[] segSizes = new int[segCount];
        available -= headerSize;
        int total = 0;
        for (int idx = 0; idx < segCount; idx++) {
            final int segSize = in.readIntLE() * 8;
            segSizes[idx] = segSize;
            total += segSize;
            if (total > MAX_TOTAL_SIZE) {
                in.resetReaderIndex();
                throw new IOException("Too much data: " + total);
            }
        }
        // Same as in encode: the segments will start on an 8-byte boundary, so if we've got an even
        // number of segments then those lengths, plus the segCount will leave us with some padding
        // to drop here.
        if (segCount % 2 == 0) {
            if (available < 4) {
                in.resetReaderIndex();
                return;
            }
            in.readIntLE();
            available -= 4;
        }
        if (available < total) {
            in.resetReaderIndex();
            return;
        }
        int readerIndex = in.readerIndex();
        final ByteBuffer[] segmentSlices = new ByteBuffer[segCount];
        for (int idx = 0; idx < segSizes.length; idx++) {
            final int segSize = segSizes[idx];
            final ByteBuffer buf = in.nioBuffer(readerIndex, segSize);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            segmentSlices[idx] = buf;
            readerIndex += segSize;
        }
        in.readerIndex(readerIndex);
        out.add(new MessageReaderRefCount(in, new MessageReader(segmentSlices, ReaderOptions.DEFAULT_READER_OPTIONS)));
    }
}
