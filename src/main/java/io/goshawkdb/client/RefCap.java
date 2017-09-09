package io.goshawkdb.client;

import io.goshawkdb.client.capnp.CapabilitiesCap;

final public class RefCap {
    public final VarUUId vUUId;
    public final Capability capability;

    RefCap(final VarUUId varUUId, final CapabilitiesCap.Capability.Reader capReader) {
        vUUId = varUUId;
        capability = Capability.fromCapnp(capReader);
    }

    RefCap(final VarUUId varUUId, final Capability cap) {
        vUUId = varUUId;
        capability = cap;
    }

    @Override
    public String toString() {
        return vUUId.toString() + "(" + capability.toString() + ")";
    }

    public RefCap denyRead() {
        return new RefCap(vUUId, capability.denyRead());
    }

    public RefCap denyWrite() {
        return new RefCap(vUUId, capability.denyWrite());
    }

    public boolean canRead() {
        return capability.canRead();
    }

    public boolean canWrite() {
        return capability.canWrite();
    }

    public boolean sameReferent(final RefCap that) {
        return this.vUUId.equals(that.vUUId);
    }

    public RefCap grantCapability(final Capability capability) {
        return new RefCap(vUUId, capability);
    }
}
