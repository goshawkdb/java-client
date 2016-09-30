package io.goshawkdb.client;

import io.goshawkdb.client.capnp.CapabilitiesCap;

/**
 * GoshawkDB provides security through use of Object Capabilities. A reference / pointer to an
 * Object within GoshawkDB contains a capability. These capabilities grant the ability for a client
 * to read, write, both or neither, an object. The GoshawkDB server enforces that all actions within
 * a transaction are legal based on the capabilities the client has received.
 */
public enum Capability {
    /**
     * An ObjectRef with the None capability grants you no actions on
     * the object.
     */
    None,
    /**
     * An ObjectRef with the Read capability grants you the ability to
     * read the object value, its version and its references.
     */
    Read,
    /**
     * An ObjectRef with the Write capability grants you the ability to
     * set (write) the object value and references.
     */
    Write,
    /**
     * An ObjectRef with the ReadWrite capability grants you both the
     * Read and Write capabilities.
     */
    ReadWrite;

    Capability union(Capability that) {
        if (this == that) {
            return this;
        } else if (this == ReadWrite || that == ReadWrite) {
            return ReadWrite;
        } else if (this == None) {
            return that;
        } else if (that == None) {
            return this;
        } else {
            // We know this != that, we know neither are ReadWrite, and we know neither are None.
            // So by dfn, one must be Read and the other Write.
            return ReadWrite;
        }
    }

    /**
     * Tests to see if the capability is either Read or ReadWrite.
     *
     * @return true iff this == Read || this == ReadWrite;
     */
    public boolean canRead() {
        return this == Read || this == ReadWrite;
    }

    /**
     * Tests to see if the capability is either Write or ReadWrite.
     *
     * @return true iff this == Write || this == ReadWrite;
     */
    public boolean canWrite() {
        return this == Write || this == ReadWrite;
    }

    static Capability fromCapnp(final CapabilitiesCap.Capability.Reader cap) {
        switch (cap.which()) {
            case NONE:
                return None;
            case READ:
                return Read;
            case WRITE:
                return Write;
            case READ_WRITE:
                return ReadWrite;
            default:
                throw new IllegalArgumentException("Unexpected capability type");
        }
    }

    void toCapnp(final CapabilitiesCap.Capability.Builder builder) {
        switch (this) {
            case None:
                builder.setNone(null);
                break;
            case Read:
                builder.setRead(null);
                break;
            case Write:
                builder.setWrite(null);
                break;
            case ReadWrite:
                builder.setReadWrite(null);
                break;
        }
    }
}
