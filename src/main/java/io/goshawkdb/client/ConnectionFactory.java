package io.goshawkdb.client;

import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;

/**
 * This class is used to construct connections to a GoshawkDB node or cluster.
 */
public class ConnectionFactory {

    public static final int DEFAULT_PORT = 7894;
    static final String PRODUCT_NAME = "GoshawkDB";
    static final String PRODUCT_VERSION = "dev";
    static final int BUFFER_SIZE = 131072;
    static final int HEARTBEAT_INTERVAL = 2;
    static final TimeUnit HEARTBEAT_INTERVAL_UNIT = TimeUnit.SECONDS;
    static final int KEY_LEN = 20;
    static final TxnId VERSION_ZERO = new TxnId(new byte[KEY_LEN]);

    public static final HashedWheelTimer timer = new HashedWheelTimer();

    public final EventLoopGroup group;

    /**
     * Create a new ConnectionFactory using a new {@link NioEventLoopGroup}
     */
    public ConnectionFactory() {
        this(new NioEventLoopGroup());
    }

    /**
     * Create a new ConnectionFactory
     *
     * @param group the netty {@link EventLoopGroup} to use
     */
    public ConnectionFactory(final EventLoopGroup group) {
        this.group = group;
    }

    /**
     * Create and start a connection to a GoshawkDB node using the default port (7894)
     *
     * @param certs The certificates to use for mutual authentication
     * @param host  The host to connect to (host name or IP address). This can be in host:port
     *              format
     * @return a new connection
     * @throws InterruptedException if an interruption occurs during connection
     */
    public Connection connect(final Certs certs, final String host) throws InterruptedException {
        final int idx = host.lastIndexOf(':');
        if (idx != -1) {
            final String portStr = host.substring(idx + 1);
            if (portStr.matches("^\\d+$")) {
                return connect(certs, host.substring(0, idx), Integer.valueOf(portStr));
            }
        }
        return connect(certs, host, DEFAULT_PORT);
    }

    /**
     * Create and start a connection to a GoshawkDB node using the specified port
     *
     * @param certs The certificates to use for mutual authentication
     * @param host  The host to connect to (host name or IP address)
     * @param port  The port to connect to
     * @return a new connection
     * @throws InterruptedException if an interruption occurs during connection
     */
    public Connection connect(final Certs certs, final String host, final int port) throws InterruptedException {
        final Connection conn = new Connection(this, certs, host, port);
        conn.connect();
        return conn;
    }
}
