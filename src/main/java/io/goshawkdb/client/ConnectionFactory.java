package io.goshawkdb.client;

import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;

public class ConnectionFactory {

    public static final int DEFAULT_PORT = 7894;
    static final String PRODUCT_NAME = "GoshawkDB";
    static final String PRODUCT_VERSION = "dev";
    static final int BUFFER_SIZE = 131072;
    static final int HEARTBEAT_INTERVAL = 2;
    static final TimeUnit HEARTBEAT_INTERVAL_UNIT = TimeUnit.SECONDS;
    static final int KEY_LEN = 20;

    static final HashedWheelTimer timer = new HashedWheelTimer();

    public final EventLoopGroup group;

    public ConnectionFactory() {
        this(new NioEventLoopGroup());
    }

    public ConnectionFactory(final EventLoopGroup group) {
        this.group = group;
    }

    public Connection connect(final Certs certs, final String host) throws InterruptedException {
        return connect(certs, host, DEFAULT_PORT);
    }

    public Connection connect(final Certs certs, final String host, final int port) throws InterruptedException {
        final Connection conn = new Connection(this, certs, host, port);
        conn.connect();
        return conn;
    }
}
