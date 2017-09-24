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

import static io.goshawkdb.client.capnp.TransactionCap.ClientActionType.CREATE;

final class Cache {
    static class ValueRef {
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
                switch (action.getActionType()) {
                    case WRITE_ONLY:
                    case READ_WRITE:
                    case CREATE: {
                        final boolean isCreate = action.getActionType() == CREATE;
                        final TransactionCap.ClientAction.Modified.Reader mod = action.getModified();
                        final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs = mod.getReferences();
                        updateFromWrite(vUUId, mod.getValue(), refs, null, isCreate);
                        break;
                    }
                    case READ_ONLY:
                        break;
                    default:
                        throw new IllegalStateException("Unexpected action type: " + action.getActionType());
                }
            }
        }
    }

    List<VarUUId> updateFromTxnAbort(final StructList.Reader<TransactionCap.ClientAction.Reader> actions, final MessageReaderRefCount reader) {
        final ArrayList<VarUUId> modifiedVars = new ArrayList<>(actions.size());
        synchronized (lock) {
            actions.forEach((final TransactionCap.ClientAction.Reader action) -> {
                final VarUUId vUUId = new VarUUId(action.getVarId().asByteBuffer());
                switch (action.getActionType()) {
                    case DELETE: {
                        updateFromDelete(vUUId);
                        break;
                    }
                    case WRITE_ONLY: {
                        // We're missing TxnId and TxnId made a write of id
                        // (to version TxnId) (though we no longer have
                        // versions in the client).
                        final TransactionCap.ClientAction.Modified.Reader mod = action.getModified();
                        final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs = mod.getReferences();
                        if (updateFromWrite(vUUId, mod.getValue(), refs, reader, false)) {
                            modifiedVars.add(vUUId);
                        }
                        break;
                    }
                }
            });
        }
        return modifiedVars;
    }

    private void updateFromDelete(final VarUUId vUUId) {
        final ValueRef vr = m.get(vUUId);
        if (vr == null || vr.references == null) {
            throw new IllegalStateException("Divergence discovered on deletion of " + vUUId + ": server thinks we had it cached, but we don't!");
        } else {
            // nb we do not wipe out the capabilities nor the vr itself!
            vr.value = null;
            vr.references = null;
            if (vr.reader != null) {
                vr.reader.release();
                vr.reader = null;
            }
        }
    }

    private boolean updateFromWrite(final VarUUId vUUId, final Data.Reader value, final StructList.Reader<TransactionCap.ClientVarIdPos.Reader> refs, final MessageReaderRefCount reader, final boolean created) {
        ValueRef vr = m.get(vUUId);
        final boolean updated = vr != null && vr.references != null;
        final RefCap[] references = new RefCap[refs.size()];
        if (vr == null && created) {
            vr = new ValueRef();
            m.put(vUUId, vr);
        } else if (vr == null) {
            throw new IllegalStateException("Received update for unknown vUUId " + vUUId);
        }
        // Must use the new array because there could be txns in
        // progress that still have pointers to the old array.
        vr.references = references;
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
