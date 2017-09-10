package io.goshawkdb.client;

/**
 * Thrown to indicate that the current transaction has been aborted.
 */
public class TransactionAbortedException extends RuntimeException {
    public static final TransactionAbortedException e = new TransactionAbortedException();

    public TransactionAbortedException() {
    }

    public TransactionAbortedException(final Exception e) {
        super(e);
    }
}
