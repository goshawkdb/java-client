package io.goshawkdb.client;

import java.nio.ByteBuffer;

import static io.goshawkdb.client.GoshawkObj.cloneByteBuffer;

class ObjectState {

    final GoshawkObj obj;

    ObjectState parent = null;
    Transaction transaction = null;
    TxnId curVersion = null;
    ByteBuffer curValue = null;
    GoshawkObj[] curObjectRefs = null;

    final boolean create;
    boolean read = false;
    boolean write = false;

    ObjectState(GoshawkObj gObj, Transaction txn, ByteBuffer val, GoshawkObj[] refs, boolean created) {
        obj = gObj;
        create = created;
        transaction = txn;
        curValue = cloneByteBuffer(val);
        if (refs == null) {
            curObjectRefs = new GoshawkObj[0];
        } else {
            curObjectRefs = new GoshawkObj[refs.length];
            System.arraycopy(refs, 0, curObjectRefs, 0, refs.length);
        }
    }

    ObjectState(GoshawkObj gObj, Transaction txn) {
        obj = gObj;
        create = false;
        transaction = txn;
    }

    ObjectState clone(Transaction txn) {
        final ObjectState os = new ObjectState(obj, txn, curValue, curObjectRefs, create);
        os.parent = this;
        os.curVersion = curVersion;
        os.read = read;
        os.write = write;
        return os;
    }
}
