package io.goshawkdb.client;

import java.util.function.Function;

/**
 * Implement this interface to create transaction funs that can be passed to {@link Connection}'s
 * runTransaction. This can neatly be done inline using a lambda expression. The transaction may be
 * automatically run several times if the server rejects the transaction for various reasons.
 * Therefore your transaction must not have any side effects until after the transaction has
 * committed (or you have chosen for the transaction to abort).
 */
@FunctionalInterface
public interface TransactionFunction<R> extends Function<Transaction, R> {
    /**
     * The callback invoked (potentially several times) to run your transaction. If you wish to
     * abort the transaction, throw a {@link TransactionAbortedException} from within the
     * transaction.
     *
     * @param txn The API through which your transaction can navigate and interact with the
     *            object-graph stored by GoshawkDB.
     * @return A result
     */
    @Override
    R apply(final Transaction txn);
}
