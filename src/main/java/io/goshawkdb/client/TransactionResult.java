package io.goshawkdb.client;

import java.util.function.BiFunction;

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
     * instead. If you're in a method where you don't know if you're inside a transaction or not,
     * the rule of thumb is to call neither, and return the TransactionResult itself.
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
     * within a transaction, use this method to inspect the result of nested transactions. If you're
     * in a method where you don't know if you're inside a transaction or not, the rule of thumb is
     * to call neither, and return the TransactionResult itself.
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
    public <S> TransactionResult<S> extend(final S r, final Exception e) {
        return new TransactionResult<>(r, txnid, e);
    }

    /**
     * Convenience method for processing the result with a continuation lambda. This is close to
     * being the Functor fmap function over an Either monad, but not quite. In a Functor over
     * Either, you would not expect the continuation to be invoked in the error case. Here it is.
     * But, because you cannot throw an Exception from within a Java lambda, if the current
     * TransactionResult has errored then the error is always passed into the returned
     * TransactionResult (there is no other way for the continuation to pass the exception through
     * to the new TransactionResult). So you can use the continuation to do tidying up of mutated
     * state, but you can't use it to absorb an error completely. If the continuation throws a
     * {@link RuntimeException} then that is caught and the TransactionResult returned will contain
     * a null value and the caught exception as the cause.
     * <p>
     * This is a synchronous method, and it is not cancellable. Thus it bears little in common with
     * {@link java.util.concurrent.Future}s or promises.
     * <p>
     * Finally, you probably still want to either return the resulting TransactionResult, or call
     * one of getResultOrAbort or getResultOrRethrow on it.
     *
     * @param after The continuation to run with the result and exception of the current
     *              TransactionResult.
     * @param <S> The type of the new result
     * @return A new TransactionResult containing the result of the after function, and the
     * exception of the current TransactionResult, unless the after function throws a {@link
     * RuntimeException} itself.
     */
    public <S> TransactionResult<S> andThen(final BiFunction<? super R, ? super Exception, ? extends S> after) {
        try {
            return extend(after.apply(result, cause), cause);
        } catch (final Exception e) {
            return extend(null, e);
        }
    }
}
