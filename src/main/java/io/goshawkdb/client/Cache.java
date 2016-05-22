package io.goshawkdb.client;

import org.capnproto.Data;
import org.capnproto.DataList;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import io.goshawkdb.client.capnp.TransactionCap;

final class Cache {
    static class ValueRef {
        TxnId version;
        ByteBuffer value;
        VarUUId[] references;
    }

    private final Object lock = new Object();
    private final HashMap<VarUUId, ValueRef> m = new HashMap<VarUUId, ValueRef>();

    Cache() {
    }

    ValueRef get(final VarUUId vUUId) {
        synchronized (lock) {
            return m.get(vUUId);
        }
    }

    void updateFromTxnCommit(final TransactionCap.ClientTxn.Reader txn, final TxnId txnId) {
        final Iterator<TransactionCap.ClientAction.Reader> actionIt = txn.getActions().iterator();
        synchronized (lock) {
            while (actionIt.hasNext()) {
                final TransactionCap.ClientAction.Reader action = actionIt.next();
                final VarUUId vUUId = new VarUUId(action.getVarId().asByteBuffer());
                switch (action.which()) {
                    case WRITE: {
                        final TransactionCap.ClientAction.Write.Reader write = action.getWrite();
                        final DataList.Reader refs = write.getReferences();
                        updateFromWrite(txnId, vUUId, write.getValue(), refs);
                        break;
                    }
                    case READWRITE: {
                        final TransactionCap.ClientAction.Readwrite.Reader rw = action.getReadwrite();
                        final DataList.Reader refs = rw.getReferences();
                        updateFromWrite(txnId, vUUId, rw.getValue(), refs);
                        break;
                    }
                    case CREATE: {
                        final TransactionCap.ClientAction.Create.Reader create = action.getCreate();
                        final DataList.Reader refs = create.getReferences();
                        updateFromWrite(txnId, vUUId, create.getValue(), refs);
                        break;
                    }
                }
            }
        }
    }

    List<VarUUId> updateFromTxnAbort(final StructList.Reader<TransactionCap.ClientUpdate.Reader> updates) {
        final ArrayList<VarUUId> modifiedVars = new ArrayList<VarUUId>(updates.size());
        final Iterator<TransactionCap.ClientUpdate.Reader> updatesIt = updates.iterator();
        synchronized (lock) {
            while (updatesIt.hasNext()) {
                final TransactionCap.ClientUpdate.Reader update = updatesIt.next();
                final TxnId txnId = new TxnId(update.getVersion().asByteBuffer());
                final StructList.Reader<TransactionCap.ClientAction.Reader> actions = update.getActions();
                final Iterator<TransactionCap.ClientAction.Reader> actionsIt = actions.iterator();
                while (actionsIt.hasNext()) {
                    final TransactionCap.ClientAction.Reader action = actionsIt.next();
                    final VarUUId vUUId = new VarUUId(action.getVarId().asByteBuffer());
                    switch (action.which()) {
                        case DELETE: {
                            updateFromDelete(vUUId, txnId);
                            break;
                        }
                        case WRITE: {
                            // We're missing TxnId and TxnId made a write of id (to
                            // version TxnId).
                            final TransactionCap.ClientAction.Write.Reader write = action.getWrite();
                            final DataList.Reader refs = write.getReferences();
                            if (updateFromWrite(txnId, vUUId, write.getValue(), refs)) {
                                modifiedVars.add(vUUId);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return modifiedVars;
    }

    private void updateFromDelete(final VarUUId vUUId, final TxnId txnId) {
        final ValueRef vr = m.remove(vUUId);
        if (vr == null) {
            throw new IllegalStateException("Divergence discovered on deletion of " + vUUId + ": server thinks we had it cached, but we don't!");
        } else if (vr.version.equals(txnId)) {
            throw new IllegalStateException("Divergence discovered on deletion of " + vUUId + ": server thinks we don't have " + txnId + " but we do!");
        }
    }

    private boolean updateFromWrite(final TxnId txnId, final VarUUId vUUId, final Data.Reader value, final DataList.Reader refs) {
        ValueRef vr = m.get(vUUId);
        final boolean missing = vr == null;
        final VarUUId[] references = new VarUUId[refs.size()];
        if (missing) {
            vr = new ValueRef();
            vr.references = references;
            m.put(vUUId, vr);
        } else if (vr.version.equals(txnId)) {
            throw new IllegalStateException("Divergence discovered on update of " + vUUId + ": server thinks we don't have " + txnId + " but we do!");
        } else {
            // Must use the new array because there could be txns in
            // progress that still have pointers to the old array.
            vr.references = references;
        }
        vr.version = txnId;
        vr.value = value.asByteBuffer().asReadOnlyBuffer();
        final Iterator<Data.Reader> refsIt = refs.iterator();
        int idx = 0;
        while (refsIt.hasNext()) {
            vr.references[idx] = new VarUUId(refsIt.next().asByteBuffer());
        }
        return !missing;
    }

}
