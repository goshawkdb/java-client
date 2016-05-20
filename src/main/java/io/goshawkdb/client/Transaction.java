package io.goshawkdb.client;

import org.capnproto.MessageReader;
import org.capnproto.StructList;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

public class Transaction<Result> extends ChannelDuplexHandler {

    private final TransactionFun<Result> fun;
    private final Connection conn;
    private final VarUUId root;
    private final Transaction parent;

    Transaction(final TransactionFun<Result> fun, final Connection conn, final VarUUId root, final Transaction parent) {
        this.fun = fun;
        this.conn = conn;
        this.root = root;
        this.parent = parent;
    }

    Result run() {
        return null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MessageReader) {
            MessageReader read = (MessageReader) msg;
            final ConnectionCap.ClientMessage.Reader result = read.getRoot(ConnectionCap.ClientMessage.factory);
            if (result.isClientTxnOutcome()) {
                ctx.pipeline().remove(this);
                // todo: process result
                TransactionCap.ClientTxn.Reader actions = result.getClientTxnSubmission();
                return;
            }
        }
        super.channelRead(ctx, msg);
    }
}
