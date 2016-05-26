package io.goshawkdb.client;

import java.util.List;

import io.goshawkdb.client.capnp.TransactionCap;

class TxnSubmissionResult {

    TransactionCap.ClientTxnOutcome.Reader outcome;
    List<VarUUId> modifiedVars;

    TxnSubmissionResult() {
    }
}
