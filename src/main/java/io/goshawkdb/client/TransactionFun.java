package io.goshawkdb.client;

/**
 * Implement this interface to create transaction funs that can be passed to {@link Connection}'s
 * runTransaction. This can neatly be done inline using a lambda expression.
 */
public interface TransactionFun<Result> {
    /**
     * The callback invoked (potentially several times) to run your transaction.
     *
     * @param txn The API through which your transaction can navigate and interact with the
     *            object-graph stored by GoshawkDB.
     * @return A result
     * @throws Throwable your callback may throw an exception.
     */
    Result Run(final Transaction txn) throws Throwable;
}
