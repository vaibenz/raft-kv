package raftkv.lsm;

import java.util.Arrays;
import java.util.Comparator;

/** A key and its value, or a tombstone ({@code value == null}) recording that the key was deleted. */
public record Entry(byte[] key, byte[] value) {
    /** Keys are ordered as unsigned bytes, so the order matches UTF-8 string order. */
    public static final Comparator<byte[]> KEY_ORDER = Arrays::compareUnsigned;

    public static Entry tombstone(byte[] key) {
        return new Entry(key, null);
    }

    public boolean isTombstone() {
        return value == null;
    }

    /** Bytes of user data this entry carries, used for memtable sizing and write amplification. */
    public int size() {
        return key.length + (value == null ? 0 : value.length);
    }
}
