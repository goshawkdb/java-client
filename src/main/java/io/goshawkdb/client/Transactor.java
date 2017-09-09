package io.goshawkdb.client;

public interface Transactor {
    <R> TransactionResult<R> transact(final TransactionFunction<? extends R> fun);
}
