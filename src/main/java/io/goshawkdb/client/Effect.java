package io.goshawkdb.client;

import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.function.Function;

import io.goshawkdb.client.capnp.TransactionCap;

final class Effect {

    Cache.ValueRef root; // nb will be null if the txn has created this object fresh.
    ByteBuffer curValue;
    RefCap[] curRefs;
    boolean origRead;
    boolean origWritten;

    Effect(final Effect e) {
        root = e.root;
        curValue = e.curValue;
        curRefs = e.curRefs;
        origRead = e.origRead;
        origWritten = e.origWritten;
    }

    Effect(final Cache.ValueRef vr) {
        root = vr;
        curValue = vr.value;
        curRefs = vr.references;
    }

    Effect(ByteBuffer value, RefCap[] refs) {
        curValue = value;
        curRefs = refs;
    }

    void setupRefs(Function<Integer, org.capnproto.StructList.Builder<io.goshawkdb.client.capnp.TransactionCap.ClientVarIdPos.Builder>> fun) {
        final StructList.Builder<TransactionCap.ClientVarIdPos.Builder> refs = fun.apply(curRefs.length);
        for (int idx = 0; idx < curRefs.length; idx++) {
            final TransactionCap.ClientVarIdPos.Builder ref = refs.get(idx);
            final RefCap rc = curRefs[idx];
            ref.setVarId(rc.vUUId.id);
            rc.capability.toCapnp(ref.initCapability());
        }
    }
}
