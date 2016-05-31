package io.goshawkdb.client;

import java.util.function.Function;

/**
 * Implement this interface to create transaction funs that can be passed to {@link Connection}'s
 * runTransaction. This can neatly be done inline using a lambda expression.
 */
@FunctionalInterface
public interface TransactionFunction<R> extends Function<Transaction, R> {
    /**
     * The callback invoked (potentially several times) to run your transaction. If you wish to
     * abort the transaction, throw an exception from within.
     *
     * @param txn The API through which your transaction can navigate and interact with the
     *            object-graph stored by GoshawkDB.
     * @return A result
     * @throws Exception your callback may throw an exception, for example to indicate you wish the
     *                   transaction to be aborted.
     */
    @Override
    R apply(final Transaction txn);
}
