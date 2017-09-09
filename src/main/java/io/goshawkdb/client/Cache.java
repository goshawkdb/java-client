package io.goshawkdb.client;

import org.capnproto.Data;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.goshawkdb.client.capnp.TransactionCap;

final class Cache {
    static class ValueRef {
        TxnId version;
        ByteBuffer value;
        RefCap[] references;
        MessageReaderRefCount reader;
        Capability cap;
    }

    private final Object lock = new Object();
    private final HashMap<VarUUId, ValueRef> m = new HashMap<>();

    Cache() {
    }

    void clear() {
        m.forEach(((vUUId, valueRef) -> {
            if (valueRef.reader != null) {
                valueRef.reader.release();
            }
        }));
        m.clear();
    }

    void setRoots(final Map<String, RefCap> roots) {
        roots.forEach((name, rc) -> {
            final ValueRef vr = new ValueRef();
            vr.cap = rc.capability;
            m.put(rc.vUUId, vr);
        });
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
                        final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs = write.getReferences();
                        updateFromWrite(txnId, vUUId, write.getValue(), refs, null, false);
                        break;
                    }
                    case READWRITE: {
                        final TransactionCap.ClientAction.Readwrite.Reader rw = action.getReadwrite();
                        final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs = rw.getReferences();
                        updateFromWrite(txnId, vUUId, rw.getValue(), refs, null, false);
                        break;
                    }
                    case CREATE: {
                        final TransactionCap.ClientAction.Create.Reader create = action.getCreate();
                        final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs = create.getReferences();
                        updateFromWrite(txnId, vUUId, create.getValue(), refs, null, true);
                        break;
                    }
                }
            }
        }
    }

    List<VarUUId> updateFromTxnAbort(final StructList.Reader<TransactionCap.ClientUpdate.Reader> updates, final MessageReaderRefCount reader) {
        final ArrayList<VarUUId> modifiedVars = new ArrayList<>(updates.size());
        final Iterator<TransactionCap.ClientUpdate.Reader> updatesIt = updates.iterator();
        synchronized (lock) {
            while (updatesIt.hasNext()) {
                final TransactionCap.ClientUpdate.Reader update = updatesIt.next();
                final TxnId txnId = new TxnId(update.getVersion().asByteBuffer());
                final StructList.Reader<TransactionCap.ClientAction.Reader> actions = update.getActions();
                actions.forEach((final TransactionCap.ClientAction.Reader action) -> {
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
                            final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs = write.getReferences();
                            if (updateFromWrite(txnId, vUUId, write.getValue(), refs, reader, false)) {
                                modifiedVars.add(vUUId);
                            }
                            break;
                        }
                    }
                });
            }
        }
        return modifiedVars;
    }

    private void updateFromDelete(final VarUUId vUUId, final TxnId txnId) {
        final ValueRef vr = m.get(vUUId);
        if (vr == null || vr.version == null) {
            throw new IllegalStateException("Divergence discovered on deletion of " + vUUId + ": server thinks we had it cached, but we don't!");
        } else if (vr.version.equals(txnId)) {
            throw new IllegalStateException("Divergence discovered on deletion of " + vUUId + ": server thinks we don't have " + txnId + " but we do!");
        } else {
            // nb we do not wipe out the capabilities nor the vr itself!
            vr.version = null;
            vr.value = null;
            vr.references = null;
            if (vr.reader != null) {
                vr.reader.release();
                vr.reader = null;
            }
        }
    }

    private boolean updateFromWrite(final TxnId txnId, final VarUUId vUUId, final Data.Reader value, final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs, final MessageReaderRefCount reader, final boolean created) {
        ValueRef vr = m.get(vUUId);
        final boolean updated = vr != null && vr.references != null;
        final RefCap[] references = new RefCap[refs.size()];
        if (vr == null) {
            vr = new ValueRef();
            m.put(vUUId, vr);
        } else if (vr.version != null && vr.version.equals(txnId)) {
            throw new IllegalStateException("Divergence discovered on update of " + vUUId + ": server thinks we don't have " + txnId + " but we do!");
        }
        // Must use the new array because there could be txns in
        // progress that still have pointers to the old array.
        vr.references = references;
        vr.version = txnId;
        vr.value = value.asByteBuffer().asReadOnlyBuffer().slice();
        if (reader != null) {
            reader.retain();
        }
        if (vr.reader != null) {
            vr.reader.release();
        }
        vr.reader = reader;
        if (created) {
            vr.cap = Capability.ReadWrite;
        }
        final Iterator<TransactionCap.ClientVarIdPos.Reader> refsIt = refs.iterator();
        int idx = 0;
        while (refsIt.hasNext()) {
            final TransactionCap.ClientVarIdPos.Reader ref = refsIt.next();
            final RefCap rc = new RefCap(new VarUUId(ref.getVarId().asByteBuffer()), ref.getCapability());
            references[idx] = rc;
            idx++;
            vr = m.get(rc.vUUId);
            if (vr == null) {
                vr = new ValueRef();
                vr.cap = rc.capability;
                m.put(rc.vUUId, vr);
            } else if (vr.cap == null) {
                vr.cap = rc.capability;
            } else {
                vr.cap = vr.cap.union(rc.capability);
            }
        }
        return updated;
    }

}
