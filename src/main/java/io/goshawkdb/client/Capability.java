package io.goshawkdb.client;

import io.goshawkdb.client.capnp.CapabilitiesCap;

public enum Capability {
    None, Read, Write, ReadWrite;

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

    public boolean canRead() {
        return this == Read || this == ReadWrite;
    }

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
