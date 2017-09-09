package io.goshawkdb.client;

import java.nio.ByteBuffer;

/**
 * An object of this type is supplied to {@link TransactionFunction}s to provide access to the object graph stored by
 * GoshawkDB. {@link Transaction} must not be used outside of a transaction.
 */
public interface Transaction extends Transactor {

    /**
     * Perform a retry operation. The set of objects read from in the transaction so far is determined, and the thread is
     * blocked until some other transaction modifies any of these objects. At this point, the transaction will be automatically
     * restarted in the same thread.
     */
    void retry();

    void abort();

    boolean restartNeeded();

    RefCap root(final String name);

    /**
     * Create a new object and set its value and references. The client creating an object will be granted full ReadWrite
     * {@link Capability} on the object for the lifetime of the client connection.
     *
     * @param value      The value to set the new object to. The buffer will be cloned and the contents copied. Therefore any
     *                   changes you make to this param after calling this method will be ignored (you will need to call a set
     *                   method). The copying will not alter any position, limit, capacity or marks of the value ByteBuffer.
     *                   The value is taken to be from position 0 to the current limit of the buffer.
     * @param references The list of objects to which the new object refers. Again, the array of references is copied.
     * @return The new object
     */
    RefCap create(final ByteBuffer value, final RefCap... references);

    void write(final RefCap objRef, final ByteBuffer value, final RefCap... references);

    ValueRefs read(final RefCap objRef);

    Capability objectCapability(final RefCap objRef);
}
