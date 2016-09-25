package io.goshawkdb.client;

import java.nio.ByteBuffer;

import static io.goshawkdb.client.GoshawkObj.cloneByteBuffer;

class ObjectState {

    final GoshawkObj obj;

    ObjectState parent = null;
    TransactionImpl<?> transaction = null;
    TxnId curVersion = null;
    ByteBuffer curValue = null;
    MessageReaderRefCount curValueRef = null;
    GoshawkObjRef[] curObjectRefs = null;

    final boolean create;
    boolean read = false;
    boolean write = false;

    // from creation, so does cloning of val and refs
    ObjectState(final GoshawkObj gObj, final TransactionImpl<?> txn, final ByteBuffer val, final GoshawkObjRef[] refs) {
        obj = gObj;
        create = true;
        transaction = txn;
        curValue = cloneByteBuffer(val).asReadOnlyBuffer();
        if (refs == null) {
            curObjectRefs = new GoshawkObjRef[0];
        } else {
            curObjectRefs = new GoshawkObjRef[refs.length];
            System.arraycopy(refs, 0, curObjectRefs, 0, refs.length);
        }
    }

    ObjectState(final GoshawkObj gObj, final TransactionImpl<?> txn) {
        obj = gObj;
        create = false;
        transaction = txn;
    }

    // The clone/parent version. Because we do clones of val and refs on their way out, we don't need to clone here.
    ObjectState(final ObjectState state, final TransactionImpl<?> txn) {
        obj = state.obj;
        parent = state;
        transaction = txn;
        curVersion = state.curVersion;
        curValue = state.curValue;
        curValueRef = state.curValueRef;
        if (curValueRef != null) {
            curValueRef.retain();
        }
        curObjectRefs = state.curObjectRefs;
        create = state.create;
        read = state.read;
        write = state.write;
    }
}
