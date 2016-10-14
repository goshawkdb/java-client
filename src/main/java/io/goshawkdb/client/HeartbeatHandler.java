package io.goshawkdb.client;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import org.capnproto.MessageBuilder;

class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    // can't use SimpleChannelInboundHandler because IdleStateEvent doesn't arrive via channelRead

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            switch (((IdleStateEvent) evt).state()) {
                case READER_IDLE:
                    System.out.println("Too many missing heartbeats. Closing connection.");
                    ctx.channel().close();
                    return;
                case WRITER_IDLE:
                    final MessageBuilder heartbeat = new MessageBuilder();
                    final ConnectionCap.ClientMessage.Builder msg = heartbeat.initRoot(ConnectionCap.ClientMessage.factory);
                    msg.setHeartbeat(null);
                    ctx.channel().writeAndFlush(heartbeat);
                    return;
                case ALL_IDLE:
                    // shouldn't happen...
                    break;
            }
        }
        super.userEventTriggered(ctx, evt);
    }
}
