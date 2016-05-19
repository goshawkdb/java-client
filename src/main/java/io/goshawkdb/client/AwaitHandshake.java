package io.goshawkdb.client;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;

import java.io.IOException;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;

import static io.goshawkdb.client.ConnectionFactory.PRODUCT_NAME;
import static io.goshawkdb.client.ConnectionFactory.PRODUCT_VERSION;

final class AwaitHandshake extends ChannelInboundHandlerAdapter {
    private final Connection conn;

    AwaitHandshake(final Connection c) {
        this.conn = c;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws IOException {
        final MessageBuilder msg = new MessageBuilder();
        final ConnectionCap.Hello.Builder hello = msg.initRoot(ConnectionCap.Hello.factory);
        hello.setProduct(PRODUCT_NAME);
        hello.setVersion(PRODUCT_VERSION);
        hello.setIsClient(true);

        ctx.channel().writeAndFlush(msg);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MessageReader) {
            final MessageReader read = (MessageReader) msg;
            final ConnectionCap.Hello.Reader h = read.getRoot(ConnectionCap.Hello.factory);
            if (h.getProduct().toString().equals(PRODUCT_NAME) && h.getVersion().toString().equals(PRODUCT_VERSION) && !h.getIsClient()) {
                final ChannelPipeline pipeline = ctx.pipeline();
                final SslContext clientSslCtx = conn.certs.buildClientSslContext();
                pipeline.addFirst(clientSslCtx.newHandler(ctx.channel().alloc()));
                pipeline.remove(this);
                conn.nextState(ctx);
                return;
            } else {
                ctx.close();
                throw new IOException("Failed to validate handshake message from server.");
            }
        }
        super.channelRead(ctx, msg);
    }
}
