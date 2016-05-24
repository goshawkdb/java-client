package io.goshawkdb.client;

import io.goshawkdb.client.capnp.TransactionCap;

class TxnSubmissionResult {

    TransactionCap.ClientTxnOutcome.Reader outcome;
    VarUUId[] modifiedVars;

    TxnSubmissionResult() {
    }
}
