package io.goshawkdb.client;

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
        if (msg instanceof MessageReaderRefCount) {
            final MessageReaderRefCount read = (MessageReaderRefCount) msg;
            final ConnectionCap.HelloClientFromServer.Reader hello = read.msg.getRoot(ConnectionCap.HelloClientFromServer.factory);
            if (hello != null && hello.hasRootId()) {
                ctx.pipeline().remove(this);
                try {
                    conn.serverHello(hello, ctx);
                    return;
                } finally {
                    read.release();
                }
            }
        }
        super.channelRead(ctx, msg);
    }
}
