package io.goshawkdb.client;

import org.capnproto.MessageBuilder;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.List;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;

import static io.goshawkdb.client.ConnectionFactory.VERSION_ZERO;

/**
 * Objects of this class represent nodes in the object graph stored by GoshawkDB. They can only be
 * modified within a transaction.
 */
class GoshawkObj {

    private final Connection conn;
    public final VarUUId id;
    final GoshawkObjRef objRef;
    ObjectState state;

    GoshawkObj(final VarUUId vUUId, final Capability capability, final Connection connection) {
        id = vUUId;
        objRef = new GoshawkObjRef(this, capability);
        conn = connection;
    }

    @Override
    public String toString() {
        return "GoshawkObj(" + id + ")";
    }

    ByteBuffer getValue() {
        checkCanRead();
        checkExpired();
        maybeRecordRead(false);
        return cloneByteBuffer(state.curValue);
    }

    GoshawkObjRef[] getReferences() {
        checkCanRead();
        checkExpired();
        maybeRecordRead(false);
        final GoshawkObjRef[] refs = new GoshawkObjRef[state.curObjectRefs.length];
        System.arraycopy(state.curObjectRefs, 0, refs, 0, refs.length);
        return refs;
    }

    TxnId getVersion() {
        checkCanRead();
        checkExpired();
        if (state.create) {
            return null;
        }
        maybeRecordRead(true);
        return state.curVersion;

    }

    void set(final ByteBuffer value, final GoshawkObjRef... references) {
        checkCanWrite();
        checkExpired();
        state.write = true;
        if (value == null) {
            state.curValue = ByteBuffer.allocate(0).asReadOnlyBuffer();
        } else {
            state.curValue = cloneByteBuffer(value).asReadOnlyBuffer();
        }
        if (state.curValueRef != null) {
            state.curValueRef.release();
            state.curValueRef = null;
        }
        if (references == null) {
            state.curObjectRefs = new GoshawkObjRef[0];
        } else {
            state.curObjectRefs = new GoshawkObjRef[references.length];
            System.arraycopy(references, 0, state.curObjectRefs, 0, references.length);
        }
    }

    private void maybeRecordRead(boolean ignoreWritten) {
        if (state.create || state.read || (state.write && !ignoreWritten)) {
            return;
        }
        Cache.ValueRef valueRef = state.transaction.cache.get(id);
        if (valueRef == null || valueRef.version == null) {
            final List<VarUUId> modifiedVars = loadVar(id, conn);
            if (state.transaction.varsUpdated(modifiedVars)) {
                throw TransactionRestartRequiredException.e;
            }
            valueRef = state.transaction.cache.get(id);
            if (valueRef == null || valueRef.version == null) {
                throw new IllegalStateException("Loading " + id + " failed to find value / update cache");
            }
        }
        state.read = true;
        state.curVersion = valueRef.version;
        if (!state.write) {
            state.curValue = valueRef.value.duplicate();
            if (state.curValueRef != null) {
                state.curValueRef.release();
            }
            state.curValueRef = valueRef.reader;
            if (state.curValueRef != null) {
                state.curValueRef.retain();
            }
            final GoshawkObjRef[] refs = new GoshawkObjRef[valueRef.references.length];
            int idx = 0;
            for (Cache.RefCap rc : valueRef.references) {
                final GoshawkObj obj = state.transaction.getObject(rc.vUUId, true);
                refs[idx] = new GoshawkObjRef(obj, rc.cap);
                idx++;
            }
            state.curObjectRefs = refs;
        }
    }

    private void checkExpired() {
        if (state == null) {
            throw new IllegalStateException("Use of expired object:" + id);
        } else if (state.transaction.resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
    }

    private void checkCanRead() {
        if (!objRef.cap.canRead()) {
            throw new IllegalArgumentException("Cannot read object " + id);
        }
    }

    private void checkCanWrite() {
        if (!objRef.cap.canWrite()) {
            throw new IllegalArgumentException("Cannot write object " + id);
        }
    }

    private static List<VarUUId> loadVar(final VarUUId vUUId, final Connection conn) {
        final MessageBuilder msg = new MessageBuilder();
        final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
        final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
        cTxn.setRetry(false);
        final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(1);
        final TransactionCap.ClientAction.Builder action = actions.get(0);
        action.setVarId(vUUId.id);
        action.initRead().setVersion(VERSION_ZERO.id);
        return conn.submitTransaction(msg, cTxn).modifiedVars;
    }

    static ByteBuffer cloneByteBuffer(final ByteBuffer buf) {
        if (buf == null) {
            return ByteBuffer.allocate(0);
        }
        final ByteBuffer clone = (buf.isDirect()) ?
                ByteBuffer.allocateDirect(buf.capacity()) :
                ByteBuffer.allocate(buf.capacity());
        final ByteBuffer readOnlyCopy = buf.asReadOnlyBuffer();
        readOnlyCopy.rewind();
        clone.put(readOnlyCopy);
        clone.rewind();
        return clone;
    }
}
