package io.goshawkdb.client;

/**
 * Encloses the result of the transaction (assuming it committed) with the {@link TxnId} of the
 * transaction.
 */
public class TransactionResult<Result> {
    public final Result result;
    public final TxnId txnid;

    TransactionResult(Result r, TxnId t) {
        result = r;
        txnid = t;
    }
}
