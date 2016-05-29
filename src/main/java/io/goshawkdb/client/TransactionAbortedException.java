package io.goshawkdb.client;

/**
 * Throw this exception to indicate the current transaction should be aborted.
 */
public class TransactionAbortedException extends RuntimeException {
    public static final TransactionAbortedException e = new TransactionAbortedException();
}
