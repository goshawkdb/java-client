package io.goshawkdb.client;

import org.capnproto.Data;
import org.capnproto.DataList;
import org.capnproto.MessageBuilder;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;

public class Transaction<Result> {

    final Cache cache;
    private final HashMap<VarUUId, GoshawkObj> objs = new HashMap<>();
    private final TransactionFun<Result> fun;
    private final Connection conn;
    private final VarUUId root;
    private final Transaction<?> parent;

    boolean resetInProgress = false;

    Transaction(final TransactionFun<Result> fun, final Connection conn, Cache cache, final VarUUId root, final Transaction parent) {
        this.fun = fun;
        this.conn = conn;
        this.cache = cache;
        this.root = root;
        this.parent = parent;
    }

    Result run() throws Throwable {
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
                Throwable t = null;
                Result result = null;
                try {
                    result = fun.Run(this);
                } catch (final TransactionRestartRequiredException e) {
                } catch (final Throwable e) {
                    t = e;
                }
                if (t != null) {
                    throw t;
                } else if (resetInProgress) {
                    if (parent == null || !parent.resetInProgress) {
                        continue;
                    } else {
                        throw TransactionRestartRequiredException.e;
                    }
                } else if (parent == null) {
                    if (submitToServer()) {
                        continue;
                    } else {
                        return result;
                    }
                } else {
                    moveObjsToParent();
                    return result;
                }
            }
        } finally {
            resetObjects();
        }
    }

    public void retry() {
        if (resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
        submitRetryTransaction();
        throw TransactionRestartRequiredException.e;
    }

    public GoshawkObj getRoot() {
        return getObject(root);
    }

    public GoshawkObj createObject(final ByteBuffer value, final GoshawkObj... references) {
        if (resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
        final GoshawkObj obj = new GoshawkObj(conn.nextVarUUId(), conn);
        objs.put(obj.id, obj);
        obj.state = new ObjectState(obj, this, value, references, true);
        return obj;
    }

    public GoshawkObj getObject(final VarUUId vUUId) {
        if (resetInProgress) {
            throw TransactionRestartRequiredException.e;
        }
        return getObject(vUUId, true);
    }

    private GoshawkObj getObject(final VarUUId vUUId, final boolean addToTxn) {
        GoshawkObj obj = objs.get(vUUId);
        if (obj != null) {
            return obj;
        } else if (parent != null) {
            obj = parent.getObject(vUUId, false);
            if (obj != null) {
                if (addToTxn) {
                    obj.state = obj.state.clone(this);
                    objs.put(vUUId, obj);
                }
                return obj;
            }
        }
        if (addToTxn) {
            obj = new GoshawkObj(vUUId, conn);
            objs.put(vUUId, obj);
            obj.state = new ObjectState(obj, this);
            return obj;
        } else {
            return null;
        }
    }

    boolean varsUpdated(final VarUUId[] modifiedVars) {
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
                    state.parent = state.parent.parent;
                }
                pObjs.putIfAbsent(vUUId, obj);
            }
        });
    }

    private void submitRetryTransaction() {
        final HashMap<VarUUId, ObjectState> reads = new HashMap<>();
        for (Transaction<?> ancestor = this; ancestor != null; ancestor = ancestor.parent) {
            final Transaction ancestorFinal = ancestor;
            final HashMap<VarUUId, GoshawkObj> objs = ancestor.objs;
            objs.forEach((final VarUUId vUUId, final GoshawkObj obj) -> {
                if (obj.state.transaction == ancestorFinal && obj.state.read) {
                    reads.putIfAbsent(vUUId, obj.state);
                }
            });
        }
        final MessageBuilder msg = new MessageBuilder();
        final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
        final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
        cTxn.setRetry(true);
        final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(reads.size());
        final Iterator<ObjectState> stateIt = reads.values().iterator();
        int idx = 0;
        while (stateIt.hasNext()) {
            final ObjectState state = stateIt.next();
            actions.get(idx).initRead().setVersion(state.curVersion.id);
            idx++;
        }
        conn.submitTransaction(msg, cTxn);
        for (Transaction ancestor = this; ancestor != null; ancestor = ancestor.parent) {
            ancestor.resetInProgress = true;
        }
    }

    private boolean submitToServer() {
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
            return false;
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
                    DataList.Builder refs;
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
                    for (GoshawkObj ref : state.curObjectRefs) {
                        refs.set(idy, new Data.Reader(ref.id.id));
                        idy++;
                    }
                }
            }
        }
        final TxnResult result = conn.submitTransaction(msg, cTxn);
        return result.outcome.which() == TransactionCap.ClientTxnOutcome.Which.ABORT;
    }
}
