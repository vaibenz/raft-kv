package raftkv.lsm;

import java.nio.ByteBuffer;

/**
 * A bloom filter over a table's keys, so a lookup for a key the table does not hold usually costs no disk read.
 *
 * <p>With {@code b} bits per key the optimal number of hash functions is {@code b ln 2}, and the false positive
 * rate is about {@code 0.6185^b}: roughly 1% at 10 bits per key. The k probe positions come from one 64 bit hash
 * split into two halves and combined as {@code h1 + i * h2} (Kirsch and Mitzenmacher), which is as good as k
 * independent hashes in practice and far cheaper.
 */
final class Bloom {
    private final int hashes;
    private final long bitCount;
    private final long[] bits;

    private Bloom(int hashes, long bitCount, long[] bits) {
        this.hashes = hashes;
        this.bitCount = bitCount;
        this.bits = bits;
    }

    static Bloom build(long[] keyHashes, int count, int bitsPerKey) {
        long m = Math.max(64, (long) count * bitsPerKey);
        m = (m + 63) / 64 * 64;
        int k = (int) Math.max(1, Math.min(30, Math.round(bitsPerKey * 0.69)));
        Bloom b = new Bloom(k, m, new long[(int) (m / 64)]);
        for (int i = 0; i < count; i++) b.add(keyHashes[i]);
        return b;
    }

    private void add(long h) {
        long h1 = h, h2 = (h >>> 32) | 1;
        for (int i = 0; i < hashes; i++) {
            long bit = Math.floorMod(h1 + i * h2, bitCount);
            bits[(int) (bit >>> 6)] |= 1L << bit;
        }
    }

    boolean mightContain(byte[] key) {
        long h = hash(key), h1 = h, h2 = (h >>> 32) | 1;
        for (int i = 0; i < hashes; i++) {
            long bit = Math.floorMod(h1 + i * h2, bitCount);
            if ((bits[(int) (bit >>> 6)] & (1L << bit)) == 0) return false;
        }
        return true;
    }

    /** FNV-1a over the bytes, then the MurmurHash3 finaliser to spread the bits. */
    static long hash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    byte[] encode() {
        ByteBuffer b = ByteBuffer.allocate(4 + 8 + bits.length * 8);
        b.putInt(hashes).putLong(bitCount);
        for (long w : bits) b.putLong(w);
        return b.array();
    }

    static Bloom decode(byte[] data) {
        ByteBuffer b = ByteBuffer.wrap(data);
        int k = b.getInt();
        long m = b.getLong();
        long[] words = new long[(int) (m / 64)];
        for (int i = 0; i < words.length; i++) words[i] = b.getLong();
        return new Bloom(k, m, words);
    }
}
