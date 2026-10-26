package raftkv.lsm;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Merges sorted runs into one sorted stream with one entry per key. Sources are given newest first; when several
 * hold the same key, the newest version wins and the others are skipped. Tombstones are passed through, so the
 * caller decides whether they can be dropped.
 */
final class MergingIterator implements Iterator<Entry> {
    private record Head(Entry entry, int source, Iterator<Entry> rest) {}

    private final PriorityQueue<Head> heap = new PriorityQueue<>((a, b) -> {
        int c = Entry.KEY_ORDER.compare(a.entry().key(), b.entry().key());
        return c != 0 ? c : Integer.compare(a.source(), b.source()); // same key: the newer source first
    });

    MergingIterator(List<Iterator<Entry>> newestFirst) {
        for (int i = 0; i < newestFirst.size(); i++) advance(newestFirst.get(i), i);
    }

    private void advance(Iterator<Entry> it, int source) {
        if (it.hasNext()) heap.add(new Head(it.next(), source, it));
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public Entry next() {
        Head top = heap.poll();
        if (top == null) throw new NoSuchElementException();
        advance(top.rest(), top.source());
        while (!heap.isEmpty() && Entry.KEY_ORDER.compare(heap.peek().entry().key(), top.entry().key()) == 0) {
            Head older = heap.poll(); // shadowed by the newer version
            advance(older.rest(), older.source());
        }
        return top.entry();
    }
}
