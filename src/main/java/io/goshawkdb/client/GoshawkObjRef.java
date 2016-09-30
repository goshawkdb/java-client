package io.goshawkdb.client;

import java.nio.ByteBuffer;

/**
 * This class represents references (or pointers) to Objects within GoshawkDB. GoshawkDB Objects can
 * only be read or modified from within a transaction. References include {@link Capability}s which
 * control whether an Object can be read, written, both, or neither. Multiple references to the same
 * Object can have different capabilities. A client gains capabilities on an Object by being able to
 * read other Objects that have references to an Object. An Object will accept any action (read,
 * write etc) which has been discovered.
 *
 * GoshawkObjRefs are linked to {@link Connection}s: if you're using multiple Connections, it is not
 * permitted to use the same GoshawkObjRef in both connections; you can either navigate to the same
 * object in both connections or once that is done in each connection, you can use getObject to get
 * a new GoshawkObjRef to the same object in the other connection. Within the same Connection, and
 * within nested transactions, GoshawkObjRefs may be freely reused.
 */
public class GoshawkObjRef {
    GoshawkObj obj;
    Capability cap;

    GoshawkObjRef(final GoshawkObj object, final Capability capability) {
        obj = object;
        cap = capability;
    }

    @Override
    public String toString() {
        return "GoshawkObjRef(" + obj + ")[" + cap + "]";
    }

    /**
     * Create a new GoshawkObjRef to the same database object but with different capabilities. This
     * will always succeed, but the transaction may be rejected if you attempt to grant capabilities
     * you have not received. From a ReadWrite capability, you can grant anything. From a Read
     * capability you can grant only a Read or None. From a Write capability you can grant only a
     * Write or None. From a None capability you can only grant a None.
     *
     * @param capability The capability to provide in the new reference
     * @return The new reference.
     */
    public GoshawkObjRef grantCapability(final Capability capability) {
        return new GoshawkObjRef(obj, capability);
    }

    /**
     * Gets the {@link Capability} from this reference. This may be different from the capabilities
     * discovered on the Object.
     *
     * @return The {@link Capability} from the reference.
     */
    public Capability getRefCapability() {
        return cap;
    }

    /**
     * Gets the {@link Capability} on the underlying Object. This is the union of all capabilities
     * that have been discovered in references that point to the same underlying Object.
     *
     * @return The {@link Capability} on the underlying Object.
     */
    public Capability getObjCapability() {
        return obj.objRef.cap;
    }

    /**
     * Test to see if two GoshawkObjRefs refer (point) to the same underlying Object. This method
     * does not do any comparison on the {@link Capability} within the references (which is why it's
     * not called equals).
     *
     * @param that The other {@link GoshawkObjRef}
     * @return true iff both this and that point to the same underlying GoshawkDB Object.
     */
    public boolean referencesSameAs(final GoshawkObjRef that) {
        return that != null && this.obj != null && that.obj != null && this.obj.id.equals(that.obj.id);
    }

    /**
     * Returns the current value of this object.
     *
     * @return The current value. The returned buffer is a copy of the underlying buffer, so
     * modifying it is perfectly safe. If you do modify it, you will need to call one of the set
     * methods for your modifications to take any effect. This method will error if you do not have
     * the Read {@link Capability} for this object.
     */
    public ByteBuffer getValue() {
        return obj.getValue();
    }

    /**
     * Get the objects pointed to from the current object.
     *
     * @return the array of {@link GoshawkObjRef} to which the current object refers. As with
     * getValue, the array is a copy of the underlying array, so you are safe to modify it, but you
     * will need to call one of the set methods for your modifications to take any effect. This
     * method will error if you do not have the Read {@link Capability} for this object.
     */
    public GoshawkObjRef[] getReferences() {
        return obj.getReferences();
    }

    /**
     * Get the current version of the object. This method will error if you do not have the Read
     * {@link Capability} for this object.
     *
     * @return the TxnId of the last transaction that wrote to this object. This will return null if
     * the object has been created by the current transaction.
     */
    public TxnId getVersion() {
        return obj.getVersion();
    }

    /**
     * Sets the value and references of the current object. If the value contains any references to
     * other objects, they must be explicitly declared as references otherwise on retrieval you will
     * not be able to navigate to them. Note that the order of references is stable. A null value
     * sets the value to a zero-length ByteBuffer. This method will error if you do not have the
     * Write {@link Capability} for this object.
     *
     * @param value      The value to set the object to. The buffer will be cloned and the contents
     *                   copied. Therefore any changes you make to this param after calling this
     *                   method will be ignored (you will need to call the set method again). The
     *                   copying will not alter any position, limit, capacity or marks of the value
     *                   ByteBuffer. The value is taken to be from position 0 to the current limit
     *                   of the buffer.
     * @param references The new list of objects to which the current object refers. Again, the
     *                   array of references is copied.
     */
    public void set(final ByteBuffer value, final GoshawkObjRef... references) {
        obj.set(value, references);
    }
}