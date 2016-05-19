package io.goshawkdb.client;

public class Transaction<Result> {

    private final TransactionFun<Result> fun;
    private final Connection conn;
    private final VarUUId root;
    private final Transaction parent;

    Transaction(final TransactionFun<Result> fun, final Connection conn, final VarUUId root, final Transaction parent) {
        this.fun = fun;
        this.conn = conn;
        this.root = root;
        this.parent = parent;
    }

    Result run() {
        return null;
    }
}
