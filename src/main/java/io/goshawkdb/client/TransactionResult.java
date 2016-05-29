package io.goshawkdb.client;

/**
 * Encloses the result of the transaction (assuming it committed) with the {@link TxnId} of the
 * transaction.
 *
 * @param <R> type of the result to be returned
 */
public class TransactionResult<R> {

    public final R result;
    public final TxnId txnid;
    public final Exception cause;

    TransactionResult(R r, TxnId t, Exception e) {
        result = r;
        txnid = t;
        cause = e;
    }

    /**
     * Returns true iff the transaction committed.
     */
    public boolean isSuccessful() {
        return cause == null;
    }

    /**
     * Returns true iff an exception was thrown during the execution of the transaction and that
     * exception was a {@link TransactionAbortedException}
     */
    public boolean isAborted() {
        return cause == TransactionAbortedException.e || (cause instanceof TransactionAbortedException);
    }
}
