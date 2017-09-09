package io.goshawkdb.client;

import java.nio.ByteBuffer;

public final class ValueRefs {
    public final ByteBuffer value;
    public final RefCap[] references;

    public ValueRefs(final ByteBuffer value, final RefCap[] refs) {
        this.value = value;
        this.references = refs;
    }
}
