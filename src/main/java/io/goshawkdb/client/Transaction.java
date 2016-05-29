package io.goshawkdb.client;

import java.nio.ByteBuffer;

/**
 * An object of this type is supplied to {@link TransactionFunction}s to provide access to the
 * object graph stored by GoshawkDB. {@link Transaction} must not be used outside of a transaction.
 */
public interface Transaction {

    /**
     * Perform a retry operation. The set of objects read from in the transaction is determined, and
     * the thread is blocked until some other transaction modifies any of these objects. At which
     * point, the transaction will be automatically restarted.
     */
    void retry();

    /**
     * Get the root of the object-graph. The Root Object is known to all clients and represents the
     * root of the object graph. For an object to be reachable, there must be a path to it from the
     * Root Object
     *
     * @return The Root Object.
     */
    GoshawkObj getRoot();

    /**
     * Create a new object and set its value and references.
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
    GoshawkObj createObject(final ByteBuffer value, final GoshawkObj... references);

    /**
     * Fetches the object specified by its unique object id. Note this will fail unless the client
     * has already navigated the object graph at least as far as any object that has a reference to
     * the object id. This method is not normally necessary: it is generally preferred to use the
     * References of objects to navigate.
     *
     * @param vUUId The id of the object to fetch
     * @return The object
     */
    GoshawkObj getObject(final VarUUId vUUId);
}
