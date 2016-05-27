package io.goshawkdb.client;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

final class Heartbeater extends ChannelDuplexHandler implements TimerTask {

    private final Object lock = new Object();

    private final ChannelHandlerContext context;
    private final MessageBuilder heartbeat;

    private int missedHeartbeats = 0;
    private boolean mustSendBeat = true;
    private Timeout timeout;

    Heartbeater(final ChannelHandlerContext ctx) {
        context = ctx;
        heartbeat = new MessageBuilder();
        final ConnectionCap.ClientMessage.Builder msg = heartbeat.initRoot(ConnectionCap.ClientMessage.factory);
        msg.setHeartbeat(null);
        synchronized (lock) {
            timeout = ConnectionFactory.timer.newTimeout(this, ConnectionFactory.HEARTBEAT_INTERVAL, ConnectionFactory.HEARTBEAT_INTERVAL_UNIT);
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        synchronized (lock) {
            if (timeout != null) {
                timeout.cancel();
            }
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof MessageReaderRefCount) {
            synchronized (lock) {
                missedHeartbeats = 0;
            }
            final MessageReaderRefCount read = (MessageReaderRefCount) msg;
            final ConnectionCap.ClientMessage.Reader h = read.msg.getRoot(ConnectionCap.ClientMessage.factory);
            if (h != null && h.isHeartbeat()) {
                read.release();
                return;
            }
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
        synchronized (lock) {
            mustSendBeat = false;
        }
        super.write(ctx, msg, promise);
    }

    @Override
    public void run(final Timeout t) throws Exception {
        synchronized (lock) {
            if (missedHeartbeats == 2) {
                System.out.println("Too many missing heartbeats. Closing connection.");
                if (context != null) {
                    context.channel().close();
                }
                return;
            }
            missedHeartbeats++;
            if (mustSendBeat) {
                // Because we write using our context, we won't self-trigger our own write() method.
                context.writeAndFlush(heartbeat);
            } else {
                mustSendBeat = true;
            }
            timeout = t.timer().newTimeout(this, ConnectionFactory.HEARTBEAT_INTERVAL, ConnectionFactory.HEARTBEAT_INTERVAL_UNIT);
        }
    }
}
