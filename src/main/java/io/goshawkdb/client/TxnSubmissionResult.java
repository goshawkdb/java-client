package io.goshawkdb.client;

import java.util.List;

import io.goshawkdb.client.capnp.TransactionCap;

final class TxnSubmissionResult {

    TransactionCap.ClientTxnOutcome.Reader outcome;
    List<VarUUId> modifiedVars;
    MessageReaderRefCount reader;

    TxnSubmissionResult() {
    }
}
