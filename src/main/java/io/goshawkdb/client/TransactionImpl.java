package io.goshawkdb.client;

import org.capnproto.Data;
import org.capnproto.MessageBuilder;
import org.capnproto.StructList;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.goshawkdb.client.capnp.ConnectionCap;
import io.goshawkdb.client.capnp.TransactionCap;

import static io.goshawkdb.client.ConnectionFactory.VERSION_ZERO;

final class TransactionImpl<R> implements Transaction {

    private final TransactionImpl<?> parent;
    private final Connection conn;
    private final Cache rootCache;

    private final Map<String, RefCap> roots;

    private HashMap<VarUUId, Effect> effects;

    private boolean restartNeeded;
    private boolean aborted;
    private boolean hasChild;
    private final Object lock = new Object();

    TransactionImpl(final Connection conn, final Cache cache, final Map<String, RefCap> roots, final TransactionImpl<?> parent) {
        this.conn = conn;
        this.rootCache = cache;
        this.roots = roots;
        this.parent = parent;
    }

    @Override
    public <S> TransactionResult<S> transact(TransactionFunction<? extends S> fun) {
        final TransactionImpl<S> child;
        synchronized (lock) {
            validate();
            if (hasChild) {
                throw new IllegalStateException("Transaction already in progress.");
            } else {
                hasChild = true;
                child = new TransactionImpl<>(conn, rootCache, roots, this);
            }
        }
        try {
            return child.run(fun);
        } finally {
            synchronized (lock) {
                hasChild = false;
            }
        }
    }

    @Override
    public void retry() {
        synchronized (lock) {
            validate();
            final HashMap<VarUUId, Cache.ValueRef> reads = new HashMap<>();
            for (TransactionImpl<?> ancestor = this; ancestor != null; ancestor = ancestor.parent) {
                final TransactionImpl<?> ancestorFinal = ancestor;
                ancestor.effects.forEach((final VarUUId vUUId, final Effect e) -> {
                    if (e.origRead) {
                        reads.putIfAbsent(vUUId, e.root);
                    }
                });
            }
            if (reads.size() == 0) {
                throw new IllegalStateException("Cannot retry: transaction never made any reads.");
            }
            final MessageBuilder msg = new MessageBuilder();
            final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
            final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
            cTxn.setRetry(true);
            final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(reads.size());
            int idx = 0;
            final Iterator<Map.Entry<VarUUId, Cache.ValueRef>> readsIt = reads.entrySet().iterator();
            while (readsIt.hasNext()) {
                final Map.Entry<VarUUId, Cache.ValueRef> entry = readsIt.next();
                final TransactionCap.ClientAction.Builder action = actions.get(idx);
                idx++;
                action.setVarId(entry.getKey().id);
                action.initRead().setVersion(entry.getValue().version.id);
            }
            final TxnSubmissionResult result = conn.submitTransaction(msg, cTxn);
            if (result.outcome.which() != TransactionCap.ClientTxnOutcome.Which.ABORT) {
                throw new RuntimeException("When retrying, failed to get abort outcome!");
            }
            determineRestart(result.modifiedVars);
        }
    }

    @Override
    public void abort() {
        synchronized (lock) {
            validate();
            aborted = true;
        }
    }

    @Override
    public boolean restartNeeded() {
        synchronized (lock) {
            return restartNeeded;
        }
    }

    /**
     * Get the roots of the object-graph. The Root Objects for each client are defined by the cluster configuration and
     * represent the roots of the object graphs. For an object to be reachable, the client must be able to discover and read a
     * path to it from a root object.
     *
     * @return References to the Root Objects.
     */
    @Override
    public RefCap root(String name) {
        return roots.get(name);
    }

    @Override
    public RefCap create(final ByteBuffer value, final RefCap... references) {
        synchronized (lock) {
            validate();
            final VarUUId vUUId = conn.nextVarUUId();
            final RefCap rc = new RefCap(vUUId, Capability.ReadWrite);
            final ByteBuffer curValue = cloneByteBuffer(value);
            final RefCap[] curRefs = new RefCap[references.length];
            System.arraycopy(references, 0, curRefs, 0, references.length);
            final Effect e = new Effect(curValue, curRefs);
            effects.put(vUUId, e);
            return rc;
        }
    }

    @Override
    public void write(final RefCap ref, final ByteBuffer value, final RefCap... references) {
        synchronized (lock) {
            validate();
            final Effect e = find(ref.vUUId, true);
            if (e.root != null && !e.root.cap.canWrite()) {
                throw new IllegalArgumentException("No capability to write.");
            }

            effects.put(ref.vUUId, e);
            // we wrote to the original if we haven't created it
            e.origWritten = e.origWritten || e.root != null;
            ByteBuffer curValue = cloneByteBuffer(value);
            final RefCap[] curRefs = new RefCap[references.length];
            System.arraycopy(references, 0, curRefs, 0, references.length);
            e.curValue = curValue;
            e.curRefs = curRefs;
        }
    }

    @Override
    public ValueRefs read(final RefCap ref) {
        synchronized (lock) {
            validate();
            final Effect e = find(ref.vUUId, true);
            if (e.root != null && !e.root.cap.canRead()) {
                throw new IllegalArgumentException("No capability to read.");
            }

            if (e.curRefs == null) { // we need to load it
                final List<VarUUId> modifiedVars = loadVar(ref.vUUId);
                final Cache.ValueRef vr = rootCache.get(ref.vUUId);
                e.curValue = vr.value;
                e.curRefs = vr.references;
                determineRestart(modifiedVars);
                if (restartNeeded) {
                    return null;
                }
            }

            effects.put(ref.vUUId, e);
            // we read the original if we haven't written it yet, and we didn't create it
            e.origRead = e.origRead || (!e.origWritten && e.root != null);

            final ByteBuffer value = cloneByteBuffer(e.curValue);
            final RefCap[] refs = new RefCap[e.curRefs.length];
            System.arraycopy(e.curRefs, 0, refs, 0, e.curRefs.length);
            return new ValueRefs(value, refs);
        }
    }

    @Override
    public Capability objectCapability(RefCap objRef) {
        synchronized (lock) {
            validate();
            final Effect e = find(objRef.vUUId, false);
            if (e.root == null) {
                return Capability.ReadWrite;
            } else {
                return e.root.cap;
            }
        }
    }

    private void validate() {
        if (aborted) {
            throw TransactionAbortedException.e;
        } else if (restartNeeded) {
            throw TransactionRestartNeededException.e;
        } else if (hasChild) {
            throw new IllegalStateException("Illegal attempt to modify parent transaction when child transaction exists.");
        }
    }

    private Effect find(final VarUUId vUUId, final boolean clone) {
        Effect e = effects.get(vUUId);
        if (e != null) {
            return e;
        } else if (parent != null) {
            e = parent.find(vUUId, false);
            if (clone) {
                e = new Effect(e);
            }
            return e;
        } else {
            final Cache.ValueRef vr = rootCache.get(vUUId);
            if (vr == null) {
                throw new RuntimeException("Attempt to fetch unknown object: " + vUUId.toString());
            } else {
                return new Effect(vr);
            }
        }
    }

    TransactionResult<R> run(final TransactionFunction<? extends R> fun) {
        while (true) {
            synchronized (lock) {
                restartNeeded = false;
                effects = new HashMap<>();
            }
            RuntimeException e = null;
            R value = null;
            try {
                value = fun.apply(this);
            } catch (final RuntimeException exception) {
                e = exception;
            } catch (final Exception exception) {
                e = new RuntimeException(exception);
            } finally {
                synchronized (lock) {
                    if (e != null || aborted) {
                        return new TransactionResult<>(value, aborted, e);
                    } else if (restartNeeded && (parent == null || !parent.restartNeeded)) {
                        continue;
                    } else if (restartNeeded) {
                        return new TransactionResult<>(value, aborted, e);
                    } else if (parent == null) {
                        commitToServer();
                        if (restartNeeded) {
                            continue;
                        } else {
                            return new TransactionResult<>(value, aborted, e);
                        }
                    } else {
                        commitToParent();
                        return new TransactionResult<>(value, aborted, e);
                    }
                }
            }
        }
    }

    private void commitToParent() {
        effects.forEach((vUUId, e) -> {
            final Effect pe = parent.effects.get(vUUId);
            if (pe == null) {
                parent.effects.put(vUUId, e);
            } else {
                pe.curValue = e.curValue;
                pe.curRefs = e.curRefs;
                pe.origRead = pe.origRead || e.origRead;
                pe.origWritten = pe.origWritten || e.origWritten;
            }
        });
    }

    private void commitToServer() {
        // invariant: parent == null
        if (effects.size() == 0) {
            return;
        }
        final MessageBuilder msg = new MessageBuilder();
        final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
        final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
        cTxn.setRetry(false);
        final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(effects.size());

        int idx = 0;
        final Iterator<Map.Entry<VarUUId, Effect>> effectsIt = effects.entrySet().iterator();
        while (effectsIt.hasNext()) {
            final Map.Entry<VarUUId, Effect> entry = effectsIt.next();
            final TransactionCap.ClientAction.Builder action = actions.get(idx);
            idx++;
            action.setVarId(entry.getKey().id);
            final Effect e = entry.getValue();
            if (e.root == null) {
                final TransactionCap.ClientAction.Create.Builder create = action.initCreate();
                create.setValue(new Data.Reader(e.curValue, 0, e.curValue.limit()));
                e.setupRefs(x -> create.initReferences(x));
            } else if (e.origRead && e.origWritten) {
                final TransactionCap.ClientAction.Readwrite.Builder readwrite = action.initReadwrite();
                readwrite.setVersion(e.root.version.id);
                readwrite.setValue(new Data.Reader(e.curValue, 0, e.curValue.limit()));
                e.setupRefs(x -> readwrite.initReferences(x));
            } else if (e.origRead) {
                action.initRead().setVersion(e.root.version.id);
            } else if (e.origWritten) {
                TransactionCap.ClientAction.Write.Builder write = action.initWrite();
                write.setValue(new Data.Reader(e.curValue, 0, e.curValue.limit()));
                e.setupRefs(x -> write.initReferences(x));
            } else {
                throw new RuntimeException("Effect appears to be a noop! " + entry.getKey().toString());
            }
        }
        final TxnSubmissionResult result = conn.submitTransaction(msg, cTxn);
        determineRestart(result.modifiedVars);
    }

    private void determineRestart(final List<VarUUId> modifiedVars) {
        if (modifiedVars == null || modifiedVars.size() == 0) {
            return;
        }
        if (parent == null) {
            modifiedVars.forEach(vUUId -> {
                if (effects.containsKey(vUUId)) {
                    restartNeeded = true;
                    return;
                }
            });
        } else {
            parent.determineRestart(modifiedVars);
            restartNeeded = parent.restartNeeded;
            if (!restartNeeded) {
                modifiedVars.forEach(vUUId -> {
                    if (effects.containsKey(vUUId)) {
                        restartNeeded = true;
                        return;
                    }
                });
            }
        }
    }

    private List<VarUUId> loadVar(final VarUUId vUUId) {
        final MessageBuilder msg = new MessageBuilder();
        final ConnectionCap.ClientMessage.Builder builder = msg.initRoot(ConnectionCap.ClientMessage.factory);
        final TransactionCap.ClientTxn.Builder cTxn = builder.initClientTxnSubmission();
        cTxn.setRetry(false);
        final StructList.Builder<TransactionCap.ClientAction.Builder> actions = cTxn.initActions(1);
        final TransactionCap.ClientAction.Builder action = actions.get(0);
        action.setVarId(vUUId.id);
        action.initRead().setVersion(VERSION_ZERO.id);
        final TxnSubmissionResult result = conn.submitTransaction(msg, cTxn);
        if (result.outcome.which() != TransactionCap.ClientTxnOutcome.Which.ABORT) {
            throw new RuntimeException("When loading, failed to get abort outcome!");
        }
        return result.modifiedVars;
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
