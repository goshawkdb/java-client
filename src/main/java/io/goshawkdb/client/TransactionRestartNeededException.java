package io.goshawkdb.client;

/**
 * Thrown to indicate the current transaction needs to be restarted.
 */
public class TransactionRestartNeededException extends RuntimeException {
    public static final TransactionRestartNeededException e = new TransactionRestartNeededException();

    public TransactionRestartNeededException() {
    }

    public TransactionRestartNeededException(final Exception e) {
        super(e);
    }
}
