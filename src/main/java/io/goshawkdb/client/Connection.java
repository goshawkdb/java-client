package io.goshawkdb.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static io.goshawkdb.client.ConnectionFactory.BUFFER_SIZE;
import static io.goshawkdb.client.ConnectionFactory.KEY_LEN;

public class Connection {

    private enum State {
        AwaitHandshake, AwaitServerHello, Run;
    }

    final Certs certs;
    final ConnectionFactory connFactory;

    private final Object lock = new Object();
    private final String host;
    private final int port;
    private final Bootstrap bootstrap;

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

    public <Result> Result runTransaction(final TransactionFun<Result> fun) {
        final VarUUId r;
        final Transaction oldTxn;
        synchronized (lock) {
            if (root == null) {
                throw new IllegalStateException("Unable to start transaction: root object not ready");
            }
            r = root;
            oldTxn = txn;
        }
        final Transaction<Result> curTxn = new Transaction(fun, this, r, oldTxn);
        synchronized (lock) {
            txn = curTxn;
        }
        final Result result = curTxn.run();
        synchronized (lock) {
            txn = oldTxn;
        }
        return result;
    }

    VarUUId nextVarUUId() {
        synchronized (lock) {
            nameSpace.putLong(0, nextVarUUId);
            nextVarUUId++;
            byte[] varUUIdArray = new byte[KEY_LEN];
            System.arraycopy(nameSpace.array(), 0, varUUIdArray, 0, KEY_LEN);
            return new VarUUId(varUUIdArray);
        }
    }

    void serverHello(final ConnectionCap.HelloClientFromServer.Reader hello, final ChannelHandlerContext ctx) throws InterruptedException {
        final byte[] rootId = hello.getRootId().toArray();
        if (rootId.length == 0) {
            lock.notifyAll();
            throw new IllegalStateException("Cluster is not yet formed; Root object has not been created.");
        } else if (rootId.length != KEY_LEN) {
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
}
