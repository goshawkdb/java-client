package io.goshawkdb.client;

import java.nio.ByteBuffer;

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
        curValue = val;
        curObjectRefs = refs;
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
