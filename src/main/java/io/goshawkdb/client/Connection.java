package io.goshawkdb.client;

import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static io.goshawkdb.client.ConnectionFactory.BUFFER_SIZE;
import static io.goshawkdb.client.ConnectionFactory.KEY_LEN;

public class Connection {

    @ChannelHandler.Sharable
    private class TxnSubmitter extends ChannelDuplexHandler {}

    private enum State {
        AwaitHandshake, AwaitServerHello, Run;
    }

    final Certs certs;
    final ConnectionFactory connFactory;

    private final Object lock = new Object();
    private final String host;
    private final int port;
    private final Bootstrap bootstrap;
    private final Cache cache = new Cache();

    private TxnResult liveTxn = null;

    private final ChannelDuplexHandler txnSubmitter = new TxnSubmitter() {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof MessageReader) {
                MessageReader read = (MessageReader) msg;
                final ConnectionCap.ClientMessage.Reader result = read.getRoot(ConnectionCap.ClientMessage.factory);
                if (result.isClientTxnOutcome()) {
                    ctx.pipeline().remove(this);
                    final TransactionCap.ClientTxnOutcome.Reader outcome = result.getClientTxnOutcome();
                    synchronized (lock) {
                        if (liveTxn == null) {
                            throw new IllegalStateException("Received txn outcome for unknown txn");
                        }
                        liveTxn.outcome = outcome;
                        lock.notifyAll();
                    }
                    return;
                }
            }
            super.channelRead(ctx, msg);
        }
    };

    private ChannelFuture connectFuture;
    private State state;
    private ChannelPipeline pipeline;
    private VarUUId root;
    private ByteBuffer nameSpace;
    private long nextVarUUId;
    private long nextTxnId;
    private Transaction txn;

    Connection(final ConnectionFactory cf, final Certs c, final String h, final int p) {
        port = p;
        host = h;
        certs = c;
        connFactory = cf;
        state = State.AwaitHandshake;
        bootstrap = new Bootstrap();
        bootstrap.group(cf.group);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_RCVBUF, BUFFER_SIZE);
        bootstrap.option(ChannelOption.SO_SNDBUF, BUFFER_SIZE);

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(final SocketChannel ch) throws Exception {
                final ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new CapnProtoCodec(Connection.this));
                pipeline.addLast(new AwaitHandshake(Connection.this));
            }
        });
    }

    void connect() throws InterruptedException {
        final ChannelFuture future;
        synchronized (lock) {
            connectFuture = bootstrap.connect(host, port);
            future = connectFuture;
        }
        future.sync();
        synchronized (lock) {
            while (root == null && (future.channel().isOpen() || future.channel().isActive())) {
                lock.wait();
            }
        }
    }

    public boolean isConnected() {
        synchronized (lock) {
            if (connectFuture != null) {
                return connectFuture.channel().isActive() && root != null;
            }
        }
        return false;
    }

    public void awaitClose() throws InterruptedException {
        ChannelFuture closeFuture = null;
        synchronized (lock) {
            if (connectFuture != null && (connectFuture.channel().isOpen() || connectFuture.channel().isActive())) {
                closeFuture = connectFuture.channel().closeFuture();
            }
        }
        if (closeFuture != null) {
            closeFuture.sync();
        }
    }

    public void close() throws InterruptedException {
        ChannelFuture closeFuture = null;
        synchronized (lock) {
            if (connectFuture != null && (connectFuture.channel().isOpen() || connectFuture.channel().isActive())) {
                closeFuture = connectFuture.channel().close();
            }
        }
        if (closeFuture != null) {
            closeFuture.sync();
        }
    }

    public <Result> Result runTransaction(final TransactionFun<Result> fun) throws Throwable {
        final VarUUId r;
        final Transaction oldTxn;
        synchronized (lock) {
            if (root == null) {
                throw new IllegalStateException("Unable to start transaction: root object not ready");
            }
            r = root;
            oldTxn = txn;
        }
        final Transaction<Result> curTxn = new Transaction(fun, this, this.cache, r, oldTxn);
        synchronized (lock) {
            txn = curTxn;
        }
        try {
            return curTxn.run();
        } finally {
            synchronized (lock) {
                txn = oldTxn;
            }
        }
    }

    VarUUId nextVarUUId() {
        synchronized (lock) {
            nameSpace.putLong(0, nextVarUUId);
            nextVarUUId++;
            return new VarUUId(nameSpace);
        }
    }

    void serverHello(final ConnectionCap.HelloClientFromServer.Reader hello, final ChannelHandlerContext ctx) throws InterruptedException {
        final ByteBuffer rootId = hello.getRootId().asByteBuffer();
        if (rootId.limit() == 0) {
            lock.notifyAll();
            throw new IllegalStateException("Cluster is not yet formed; Root object has not been created.");
        } else if (rootId.limit() != KEY_LEN) {
            lock.notifyAll();
            throw new IllegalStateException("Root object VarUUId is of wrong length!");
        } else {
            synchronized (lock) {
                pipeline = ctx.pipeline();
                root = new VarUUId(rootId);
                byte[] nameSpaceArray = new byte[KEY_LEN];
                System.arraycopy(hello.getNamespace().toArray(), 0, nameSpaceArray, 8, KEY_LEN - 8);
                nameSpace = ByteBuffer.wrap(nameSpaceArray);
                nameSpace.order(ByteOrder.BIG_ENDIAN);
                nextVarUUId = 0;
                lock.notifyAll();
            }
            nextState(ctx);
        }
    }

    void disconnected() {
        synchronized (lock) {
            root = null;
            lock.notifyAll();
        }
    }

    void nextState(final ChannelHandlerContext ctx) throws InterruptedException {
        synchronized (lock) {
            switch (state) {
                case AwaitHandshake: {
                    state = State.AwaitServerHello;
                    ctx.pipeline().addLast(new AwaitServerHello(this));
                    break;
                }
                case AwaitServerHello: {
                    state = State.Run;
                    ctx.pipeline().addLast(new Heartbeater(this, ctx));
                    break;
                }
                case Run: {
                    if (connectFuture != null) {
                        connectFuture.channel().close();
                        awaitClose();
                    }
                    break;
                }
            }
        }
    }

    TxnResult submitTransaction(final MessageBuilder msg, final TransactionCap.ClientTxn.Builder cTxn) {
        synchronized (lock) {
            if (state != State.Run) {
                throw new IllegalStateException("Connection in wrong state: " + state);
            } else if (liveTxn != null) {
                throw new IllegalStateException("Existing live txn");
            }
            nameSpace.putLong(0, nextTxnId);
            byte[] txnIdArray = new byte[KEY_LEN];
            System.arraycopy(nameSpace.array(), 0, txnIdArray, 0, KEY_LEN);
            cTxn.setId(txnIdArray);
            final TxnResult result = new TxnResult();
            liveTxn = result;
            pipeline.addLast(txnSubmitter);
            pipeline.writeAndFlush(msg);
            while (result.outcome == null && isConnected()) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                }
            }
            liveTxn = null;
            if (result.outcome == null) {
                throw new IllegalStateException("Connection disconnected whilst waiting txn result.");
            }
            if (!Arrays.equals(txnIdArray, result.outcome.getId().toArray())) {
                throw new IllegalStateException("Received txn outcome for wrong txn");
            }
            final ByteBuffer finalTxnIdBuf = result.outcome.getFinalId().asByteBuffer();
            finalTxnIdBuf.order(ByteOrder.BIG_ENDIAN);
            final long finalTxnIdLong = finalTxnIdBuf.getLong(0);
            if (finalTxnIdLong < nextTxnId) {
                throw new IllegalStateException("Final (" + finalTxnIdLong + ") < next (" + nextTxnId + ")");
            }
            nextTxnId = finalTxnIdLong + 1;
            final TxnId finalTxnId = new TxnId(finalTxnIdBuf);
            switch (result.outcome.which()) {
                case COMMIT: {
                    cache.updateFromTxnCommit(cTxn.asReader(),finalTxnId);
                    break;
                }
                case ABORT: {
                    cache.updateFromTxnAbort(result.outcome.getAbort());
                    break;
                }
                case ERROR: {
                    throw new IllegalStateException(result.outcome.getError().toString());
                }
            }
            return result;
        }
    }
}
