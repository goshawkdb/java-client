package io.goshawkdb.client;

import org.capnproto.Data;
import org.capnproto.MessageBuilder;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;

import static io.goshawkdb.client.ConnectionFactory.VERSION_ZERO;

final class TransactionImpl<R> implements Transaction {

    final Cache cache;
    private final HashMap<VarUUId, GoshawkObj> objs = new HashMap<>();
    private final TransactionFunction<R> fun;
    private final Connection conn;
    private final Map<String, Cache.RefCap> roots;
    private final TransactionImpl<?> parent;

    boolean resetInProgress = false;

    TransactionImpl(final TransactionFunction<R> fun, final Connection conn, Cache cache, final Map<String, Cache.RefCap> roots, final TransactionImpl<?> parent) {
        this.fun = fun;
        this.conn = conn;
        this.cache = cache;
        this.roots = roots;
        this.parent = parent;
    }

    TransactionResult<R> run() {
        try {
            while (true) {
                if (resetInProgress) {
                    if (parent == null || !parent.resetInProgress) {
                        resetInProgress = false;
                    } else {
                        throw TransactionRestartRequiredException.e;
                    }
                }
                resetObjects();
                R result = null;
                try {
                    result = fun.apply(this);
                } catch (final TransactionRestartRequiredException e) {
                } catch (final Exception e) {
                    return new TransactionResult<>(null, null, e);
                }
                if (resetInProgress) {
                    if (parent == null || !parent.resetInProgress) {
                        continue;
                    } else {
                        throw TransactionRestartRequiredException.e;
                    }
                } else if (parent == null) {
                    final TxnId txnId = submitToServer();
                    if (txnId == null) {
                        continue;
                    } else {
                        return new TransactionResult<>(result, txnId, null);
                    }
                } else {
                    moveObjsToParent();
                    return new TransactionResult<>(result, null, null);
                }
            }
        } finally {
            resetObjects();
        }
    }

    @Override
    public void retry() {
        if (resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
        submitRetryTransaction();
        throw TransactionRestartRequiredException.e;
    }

    @Override
    public Map<String, GoshawkObjRef> getRoots() {
        final Map<String, GoshawkObjRef> rootObjects = new HashMap<>();
        roots.forEach((name, rc) -> {
            final GoshawkObj obj = getObject(rc.vUUId, true);
            rootObjects.put(name, new GoshawkObjRef(obj, rc.cap));
        });
        return rootObjects;
    }

    @Override
    public GoshawkObjRef createObject(final ByteBuffer value, final GoshawkObjRef... references) {
        if (resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
        final GoshawkObj obj = new GoshawkObj(conn.nextVarUUId(), Capability.ReadWrite, conn);
        objs.put(obj.id, obj);
        obj.state = new ObjectState(obj, this, value, references);
        return obj.objRef;
    }

    @Override
    public GoshawkObjRef getObject(final GoshawkObjRef objRef) {
        if (resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
        objRef.obj = getObject(objRef.obj.id, true);
        return objRef;
    }

    GoshawkObj getObject(final VarUUId vUUId, final boolean addToTxn) {
        GoshawkObj obj = objs.get(vUUId);
        if (obj != null) {
            return obj;
        } else if (parent != null) {
            obj = parent.getObject(vUUId, false);
            if (obj != null) {
                if (addToTxn) {
                    obj.state = new ObjectState(obj.state, this);
                    objs.put(vUUId, obj);
                }
                return obj;
            }
        }
        if (addToTxn) {
            final Cache.ValueRef vr = cache.get(vUUId);
            if (vr == null) {
                throw new IllegalArgumentException("Attempt to dereference GoshawkObjRef to unknown GoshawkObj: " + vUUId);
            }
            obj = new GoshawkObj(vUUId, vr.cap, conn);
            objs.put(vUUId, obj);
            obj.state = new ObjectState(obj, this);
            return obj;
        } else {
            return null;
        }
    }

    boolean varsUpdated(final List<VarUUId> modifiedVars) {
        if (parent != null && parent.varsUpdated(modifiedVars)) {
            resetInProgress = true;
            return true;
        } else if (resetInProgress) {
            return true;
        } else if (modifiedVars != null) {
            for (VarUUId vUUId : modifiedVars) {
                final GoshawkObj obj = objs.get(vUUId);
                if (obj != null && obj.state.transaction == this && obj.state.read) {
                    resetInProgress = true;
                    return true;
                }
            }
        }
        return false;
    }

    private void resetObjects() {
        objs.forEach((final VarUUId vUUId, final GoshawkObj obj) -> {
            if (obj.state.transaction == this) {
                if (obj.state.curValueRef != null) {
                    obj.state.curValueRef.release();
                }
                obj.state = obj.state.parent;
            }
        });
        objs.clear();
    }

    private void moveObjsToParent() {
        final HashMap<VarUUId, GoshawkObj> pObjs = parent.objs;
        objs.forEach((final VarUUId vUUId, final GoshawkObj obj) -> {
            final ObjectState state = obj.state;
            if (state.transaction == this) {
                state.transaction = parent;
                if (state.parent != null && state.parent.transaction == parent) {
                    if (state.parent.curValueRef != null) {
                        state.parent.curValueRef.release();
                    }
                    state.parent = state.parent.parent;
                }
                pObjs.putIfAbsent(vUUId, obj);
            }
        });
    }

    private void submitRetryTransaction() {
        final HashMap<VarUUId, ObjectState> reads = new HashMap<>();
        for (TransactionImpl<?> ancestor = this; ancestor != null; ancestor = ancestor.parent) {
            final TransactionImpl<?> ancestorFinal = ancestor;
            final HashMap<VarUUId, GoshawkObj> objs = ancestor.objs;
            objs.forEach((final VarUUId vUUId, final GoshawkObj obj) -> {
                if (obj.state.transaction == ancestorFinal && obj.state.read) {
                    reads.putIfAbsent(vUUId, obj.state);
                }
            });
        }
        if (reads.size() > 0) {
            final MessageBuilder msg = new MessageBuilder();
            final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
            final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
            cTxn.setRetry(true);
            final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(reads.size());
            final Iterator<ObjectState> stateIt = reads.values().iterator();
            int idx = 0;
            while (stateIt.hasNext()) {
                final ObjectState state = stateIt.next();
                final TransactionCap.ClientAction.Builder action = actions.get(idx);
                action.setVarId(state.obj.id.id);
                action.initRead().setVersion(state.curVersion.id);
                idx++;
            }
            conn.submitTransaction(msg, cTxn);
        }
        for (TransactionImpl<?> ancestor = this; ancestor != null; ancestor = ancestor.parent) {
            ancestor.resetInProgress = true;
        }
    }

    private TxnId submitToServer() {
        final int s = objs.size();
        final ArrayList<ObjectState> reads = new ArrayList<>(s);
        final ArrayList<ObjectState> writes = new ArrayList<>(s);
        final ArrayList<ObjectState> readwrites = new ArrayList<>(s);
        final ArrayList<ObjectState> creates = new ArrayList<>(s);
        objs.forEach((final VarUUId vUUId, final GoshawkObj obj) -> {
            final ObjectState state = obj.state;
            if (state.create) {
                creates.add(state);
            } else if (state.read && state.write) {
                readwrites.add(state);
            } else if (state.write) {
                writes.add(state);
            } else if (state.read) {
                reads.add(state);
            }
        });
        final int totalLen = reads.size() + writes.size() + readwrites.size() + creates.size();
        if (totalLen == 0) {
            return VERSION_ZERO;
        }
        final MessageBuilder msg = new MessageBuilder();
        final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
        final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
        cTxn.setRetry(false);
        final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(totalLen);

        final ArrayList<ArrayList<ObjectState>> lists = new ArrayList<>(4);
        lists.add(reads);
        lists.add(writes);
        lists.add(readwrites);
        lists.add(creates);
        int idx = 0;
        final Iterator<ArrayList<ObjectState>> listsIt = lists.iterator();
        while (listsIt.hasNext()) {
            final ArrayList<ObjectState> list = listsIt.next();
            final Iterator<ObjectState> listIt = list.iterator();
            while (listIt.hasNext()) {
                final ObjectState state = listIt.next();
                final TransactionCap.ClientAction.Builder action = actions.get(idx);
                idx++;
                action.setVarId(state.obj.id.id);
                if (list == reads) {
                    action.initRead().setVersion(state.curVersion.id);
                } else {
                    StructList.Builder<TransactionCap.ClientVarIdPos.Builder> refs;
                    if (list == writes) {
                        TransactionCap.ClientAction.Write.Builder write = action.initWrite();
                        refs = write.initReferences(state.curObjectRefs.length);
                        write.setValue(new Data.Reader(state.curValue, 0, state.curValue.limit()));
                    } else if (list == readwrites) {
                        final TransactionCap.ClientAction.Readwrite.Builder readwrite = action.initReadwrite();
                        refs = readwrite.initReferences(state.curObjectRefs.length);
                        readwrite.setVersion(state.curVersion.id);
                        readwrite.setValue(new Data.Reader(state.curValue, 0, state.curValue.limit()));
                    } else {
                        final TransactionCap.ClientAction.Create.Builder create = action.initCreate();
                        refs = create.initReferences(state.curObjectRefs.length);
                        create.setValue(new Data.Reader(state.curValue, 0, state.curValue.limit()));
                    }
                    int idy = 0;
                    for (GoshawkObjRef ref : state.curObjectRefs) {
                        final TransactionCap.ClientVarIdPos.Builder refCap = refs.get(idy);
                        ref.cap.toCapnp(refCap.initCapability());
                        refCap.setVarId(ref.obj.id.id);
                        idy++;
                    }
                }
            }
        }
        final TxnSubmissionResult result = conn.submitTransaction(msg, cTxn);
        if (result.outcome.which() == TransactionCap.ClientTxnOutcome.Which.ABORT) {
            return null;
        } else {
            return new TxnId(result.outcome.getFinalId().asByteBuffer());
        }
    }
}
