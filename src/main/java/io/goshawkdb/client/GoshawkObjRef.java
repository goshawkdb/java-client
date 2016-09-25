package io.goshawkdb.client;

import java.nio.ByteBuffer;

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

    public GoshawkObjRef grantCapability(final Capability capability) {
        return new GoshawkObjRef(obj, capability);
    }

    public Capability getRefCapability() {
        return cap;
    }

    public Capability getObjCapability() {
        return obj.objRef.cap;
    }

    public boolean referencesSameAs(final GoshawkObjRef that) {
        return that != null && this.obj != null && that.obj != null && this.obj.id.equals(that.obj.id);
    }

    /**
     * Returns the current value of this object.
     *
     * @return The current value. The returned buffer is a copy of the underlying buffer, so
     * modifying it is perfectly safe. If you do modify it, you will need to call one of the set
     * methods for your modifications to take any effect.
     */
    public ByteBuffer getValue() {
        return obj.getValue();
    }

    /**
     * Get the objects pointed to from the current object.
     *
     * @return the array of {@link GoshawkObj} to which the current object refers. As with getValue,
     * the array is a copy of the underlying array, so you are safe to modify it, but you will need
     * to call one of the set methods for your modifications to take any effect.
     */
    public GoshawkObjRef[] getReferences() {
        return obj.getReferences();
    }

    /**
     * Get the current version of the object.
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
     * sets the value to a zero-length ByteBuffer.
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