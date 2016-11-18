package io.goshawkdb.client;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * An object of this type is supplied to {@link TransactionFunction}s to provide access to the
 * object graph stored by GoshawkDB. {@link Transaction} must not be used outside of a transaction.
 */
public interface Transaction {

    /**
     * Perform a retry operation. The set of objects read from in the transaction so far is
     * determined, and the thread is blocked until some other transaction modifies any of these
     * objects. At this point, the transaction will be automatically restarted in the same thread.
     */
    void retry();

    /**
     * Get the roots of the object-graph. The Root Objects for each client are defined by the
     * cluster configuration and represent the roots of the object graphs. For an object to be
     * reachable, the client must be able to discover and read a path to it from a root object.
     *
     * @return References to the Root Objects.
     */
    Map<String, GoshawkObjRef> getRoots();

    /**
     * Create a new object and set its value and references. The client creating an object will be
     * granted full ReadWrite {@link Capability} on the object for the lifetime of the client
     * connection.
     *
     * @param value      The value to set the new object to. The buffer will be cloned and the
     *                   contents copied. Therefore any changes you make to this param after calling
     *                   this method will be ignored (you will need to call a set method). The
     *                   copying will not alter any position, limit, capacity or marks of the value
     *                   ByteBuffer. The value is taken to be from position 0 to the current limit
     *                   of the buffer.
     * @param references The list of objects to which the new object refers. Again, the array of
     *                   references is copied.
     * @return The new object
     */
    GoshawkObjRef createObject(final ByteBuffer value, final GoshawkObjRef... references);

    /**
     * Fetches the object specified by a reference. Note this will fail unless the client has
     * already navigated the object graph at least as far as any object that has a reference to the
     * object reference provided. This method is not normally necessary: it is generally preferred
     * to use the References of objects to navigate.
     *
     * @param objRef Reference of the object to retrieve
     * @return The object. The same object ref provided is returned but it is updated as necessary
     * to a valid object. Note that this method does not validate the capability embedded within the
     * reference is valid.
     */
    GoshawkObjRef getObject(final GoshawkObjRef objRef);
}
