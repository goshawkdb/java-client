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

import static io.netty.buffer.Unpooled.wrappedBuffer;

final class CapnProtoCodec extends ByteToMessageCodec<MessageBuilder> {
    private static final int MAX_SEGMENT_NUMBER = 1024;
    private static final int MAX_TOTAL_SIZE = 1024 * 1024 * 1024;

    private final Connection conn;

    CapnProtoCodec(Connection connection) {
        conn = connection;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        conn.disconnected();
        super.channelInactive(ctx);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, MessageBuilder msg, ByteBuf out) throws Exception {
        final ByteBuffer[] segments = msg.getSegmentsForOutput();
        out.writeIntLE(segments.length - 1);

        for (ByteBuffer seg : segments) {
            out.writeIntLE(seg.limit() / 8);
        }

        out.writeBytes(wrappedBuffer(segments));
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
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
        out.add(new MessageReader(segmentSlices, ReaderOptions.DEFAULT_READER_OPTIONS));
    }
}
