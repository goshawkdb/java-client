package io.goshawkdb.client;

/**
 * Encloses the result of the transaction (assuming it committed) with the {@link TxnId} of the transaction.
 *
 * @param <R> type of the result to be returned
 */
public class TransactionResult<R> {

    public final R result;
    public final boolean aborted;
    public final RuntimeException cause;

    TransactionResult(R r, boolean abt, RuntimeException e) {
        result = r;
        aborted = abt;
        cause = e;
    }

    /**
     * Returns true iff the transaction committed.
     */
    public boolean isSuccessful() {
        return cause == null && !aborted;
    }

    /**
     * Returns the result of the transaction (which may be null) unless an exception occurred, in which case it re-throws the
     * exception. Within a transaction, you want to use getResultOrAbort instead. If you're in a method where you don't know if
     * you're inside a transaction or not, the rule of thumb is to call neither, and return the TransactionResult itself.
     *
     * @return The result of the transaction
     * @throws Exception if the transaction itself threw an exception
     */
    public R getResultOrRethrow() throws RuntimeException {
        if (cause == null) {
            return result;
        } else {
            throw cause;
        }
    }
}
