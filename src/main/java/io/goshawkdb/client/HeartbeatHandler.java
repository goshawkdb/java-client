package io.goshawkdb.client;

import org.capnproto.MessageBuilder;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

final class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    // Can't use SimpleChannelInboundHandler because IdleStateEvent doesn't arrive via channelRead

    private final MessageBuilder heartbeat = new MessageBuilder();

    HeartbeatHandler() {
        heartbeat.initRoot(ConnectionCap.ClientMessage.factory).setHeartbeat(null);
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            final IdleState state = ((IdleStateEvent) evt).state();
            switch (state) {
                case READER_IDLE:
                    System.out.println("Too many missing heartbeats. Closing connection.");
                    ctx.channel().close();
                    return;
                case WRITER_IDLE:
                    ctx.channel().writeAndFlush(heartbeat);
                    return;
                default:
                    throw new IllegalStateException("Unexpected IdleStateEvent state: " + state);
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
