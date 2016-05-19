package io.goshawkdb.client;

import org.capnproto.MessageReader;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

final class AwaitServerHello extends ChannelInboundHandlerAdapter {

    private final Connection conn;

    AwaitServerHello(final Connection connection) {
        conn = connection;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof MessageReader) {
            final MessageReader read = (MessageReader) msg;
            final ConnectionCap.HelloClientFromServer.Reader hello = read.getRoot(ConnectionCap.HelloClientFromServer.factory);
            if (hello != null && hello.hasRootId()) {
                ctx.pipeline().remove(this);
                conn.serverHello(hello, ctx);
                return;
            }
        }
        super.channelRead(ctx, msg);
    }
}
