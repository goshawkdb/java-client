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

    /**
     * Returns the result of the transaction (which may be null) unless an exception occurred, in
     * which case it re-throws the exception. Within a transaction, you want to use getResultOrAbort
     * instead.
     *
     * @return The result of the transaction
     * @throws Exception if the transaction itself threw an exception
     */
    public R getResultOrRethrow() throws Exception {
        if (cause == null) {
            return result;
        } else {
            throw cause;
        }
    }

    /**
     * Returns the result of the transaction (which may be null) unless an exception occurred, in
     * which case it throws a {@link TransactionAbortedException} wrapping the exception. When
     * within a transaction, use this method to inspect the result of nested transactions.
     *
     * @return The result of the transaction
     * @throws TransactionAbortedException if the transaction itself threw an exception
     */
    public R getResultOrAbort() throws TransactionAbortedException {
        if (cause == null) {
            return result;
        } else if (isAborted()) {
            throw (TransactionAbortedException) cause;
        } else {
            throw new TransactionAbortedException(cause);
        }
    }

    /**
     * If you want to modify the result of a transaction, use this method. It keeps the txnid.
     *
     * @param r   The new result (can be null)
     * @param e   The new exception (can be null)
     * @param <S> The type of the new result
     * @return A new TransactionResult with the same txnid as the current.
     */
    public <S> TransactionResult<S> extend(S r, Exception e) {
        return new TransactionResult<>(r, txnid, e);
    }
}
