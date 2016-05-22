package io.goshawkdb.client;

public interface TransactionFun<Result> {
    Result Run(Transaction txn) throws Throwable;
}
