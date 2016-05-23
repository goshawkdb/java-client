package io.goshawkdb.client;

import org.capnproto.MessageBuilder;
import org.capnproto.StructList;

import java.nio.ByteBuffer;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;

import static io.goshawkdb.client.ConnectionFactory.VERSION_ZERO;

/**
 * Objects of this class represent nodes in the object graph stored by GoshawkDB. They can only be
 * modified within a transaction.
 */
public class GoshawkObj {

    private final Connection conn;
    final VarUUId id;
    ObjectState state;

    GoshawkObj(final VarUUId vUUId, final Connection connection) {
        id = vUUId;
        conn = connection;
    }

    /**
     * Returns the current value of this object.
     *
     * @return The current value. The returned buffer is read only and will have current position of
     * 0.
     */
    public ByteBuffer getValue() {
        checkExpired();
        maybeRecordRead(false);
        final ByteBuffer buf = state.curValue.asReadOnlyBuffer();
        buf.rewind();
        return buf;
    }

    /**
     * Get the objects pointed to from the current object.
     *
     * @return the list of {@link GoshawkObj} to which the current object refers.
     */
    public GoshawkObj[] getReferences() {
        checkExpired();
        maybeRecordRead(false);
        final GoshawkObj[] refs = new GoshawkObj[state.curObjectRefs.length];
        System.arraycopy(state.curObjectRefs, 0, refs, 0, state.curObjectRefs.length);
        return refs;
    }

    /**
     * Get the current version of the object.
     *
     * @return the TxnId of the last transaction that wrote to this object. This will return null if
     * the object has been created by the current transaction.
     */
    public TxnId getVersion() {
        checkExpired();
        if (state.create) {
            return null;
        }
        maybeRecordRead(true);
        return state.curVersion;

    }

    /**
     * Sets the value and references of the current object. If the value contains any references to
     * other objects, they must be explicitly declared as references otherwise on retrieval you will
     * not be able to navigate to them. Note that the order of references is stable.
     *
     * @param value      The value to set the object to. The buffer will be cloned and the contents
     *                   copied. Therefore any changes you make to this param after calling this
     *                   method will be ignored. The value is taken to be from position 0 to the
     *                   current limit of the buffer.
     * @param references The new list of objects to which the current object refers.
     */
    public void set(final ByteBuffer value, final GoshawkObj... references) {
        if (value == null || references == null) {
            throw new NullPointerException("Nulls encountered in call to GoshawkObj.set");
        }
        checkExpired();
        state.write = true;
        state.curValue = cloneByteBuffer(value);
        state.curObjectRefs = new GoshawkObj[references.length];
        System.arraycopy(references, 0, state.curObjectRefs, 0, references.length);
    }

    /**
     * Sets the value of the current object. The current references are unaltered. If the value
     * contains any references to other objects, they must be explicitly declared as references
     * otherwise on retrieval you will not be able to navigate to them. Note that the order of
     * references is stable.
     *
     * @param value The value to set the object to. The buffer will be cloned and the contents
     *              copied. Therefore any changes you make to this param after calling this method
     *              will be ignored. The value is taken to be from position 0 to the current limit
     *              of the buffer.
     */
    public void setValue(final ByteBuffer value) {
        if (value == null) {
            throw new NullPointerException("Nulls encountered in call to GoshawkObj.setValue");
        }
        checkExpired();
        state.write = true;
        state.curValue = cloneByteBuffer(value);
    }

    /**
     * Sets the references of the current object. The current value is unaltered. If the value
     * contains any references to other objects, they must be explicitly declared as references
     * otherwise on retrieval you will not be able to navigate to them. Note that the order of
     * references is stable.
     *
     * @param references The new list of objects to which the current object refers.
     */
    public void setReferences(final GoshawkObj... references) {
        if (references == null) {
            throw new NullPointerException("Nulls encountered in call to GoshawkObj.setReferences");
        }
        checkExpired();
        state.write = true;
        state.curObjectRefs = new GoshawkObj[references.length];
        System.arraycopy(references, 0, state.curObjectRefs, 0, references.length);
    }

    private void maybeRecordRead(boolean ignoreWritten) {
        if (state.create || state.read || (state.write && !ignoreWritten)) {
            return;
        }
        Cache.ValueRef valueRef = state.transaction.cache.get(id);
        if (valueRef == null) {
            final VarUUId[] modifiedVars = loadVar(id, conn);
            if (state.transaction.varsUpdated(modifiedVars)) {
                throw TransactionRestartRequiredException.e;
            }
            valueRef = state.transaction.cache.get(id);
            if (valueRef == null) {
                throw new IllegalStateException("Loading " + id + " failed to find value / update cache");
            }
        }
        state.read = true;
        state.curVersion = valueRef.version;
        if (!state.write) {
            state.curValue = valueRef.value.duplicate();
            final GoshawkObj[] refs = new GoshawkObj[valueRef.references.length];
            int idx = 0;
            for (VarUUId vUUId : valueRef.references) {
                refs[idx] = state.transaction.getObject(vUUId);
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

    private static VarUUId[] loadVar(final VarUUId vUUId, final Connection conn) {
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

    private static ByteBuffer cloneByteBuffer(final ByteBuffer buf) {
        final ByteBuffer clone = (buf.isDirect()) ?
                ByteBuffer.allocateDirect(buf.capacity()) :
                ByteBuffer.allocate(buf.capacity());
        final ByteBuffer readOnlyCopy = buf.asReadOnlyBuffer();
        readOnlyCopy.rewind();
        clone.put(readOnlyCopy);
        return clone;
    }

}
