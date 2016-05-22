package io.goshawkdb.client;

import io.goshawkdb.client.capnp.TransactionCap;

class TxnResult {
    TransactionCap.ClientTxnOutcome.Reader outcome;
    VarUUId[] modifiedVars;

    TxnResult() {
    }
}
