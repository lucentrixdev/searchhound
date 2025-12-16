// InMemoryJoinIndex.java - fixed version handling
package org.apache.lucene.util;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryJoinIndex implements JoinIndex {
    private final Map<BytesRef, List<Integer>> leftToRightMap;
    private final Map<BytesRef, List<Integer>> rightToLeftMap;
    private boolean built = false;
    private final String leftField;
    private final String rightField;
    private long indexVersion = -1;
    private final AtomicLong memoryUsage = new AtomicLong(0);
    private long lastAccessTime = System.currentTimeMillis();

    public InMemoryJoinIndex(String leftField, String rightField) {
        this.leftField = leftField;
        this.rightField = rightField;
        this.leftToRightMap = new HashMap<>();
        this.rightToLeftMap = new HashMap<>();
    }

    @Override
    public void build(IndexReader leftIndex, IndexReader rightIndex) throws IOException {
        clear();

        long newVersion = calculateIndexVersion(leftIndex, rightIndex);
        if (this.indexVersion == newVersion && built) {
            updateLastAccessTime();
            return; // Already up-to-date
        }

        this.indexVersion = newVersion;

        // Build mappings using DocValues
        // Map values -> right doc ids
        buildFieldMappingWithDocValues(rightIndex, rightField, leftToRightMap);
        // Map values -> left doc ids (reverse direction)
        buildFieldMappingWithDocValues(leftIndex, leftField, rightToLeftMap);

        built = true;
        updateLastAccessTime();

        // Calculate memory usage
        calculateMemoryUsage();
    }

    private void buildFieldMappingWithDocValues(IndexReader index, String field,
                                                Map<BytesRef, List<Integer>> mapping) throws IOException {
        for (LeafReaderContext context : index.leaves()) {
            LeafReader reader = context.reader();
            SortedDocValues docValues = reader.getSortedDocValues(field);
            if (docValues == null) continue;

            int docBase = context.docBase;
            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                if (docValues.advanceExact(docId)) {
                    BytesRef termValue = BytesRef.deepCopyOf(
                            docValues.lookupOrd(docValues.ordValue()));
                    int globalDocId = docBase + docId;

                    mapping.computeIfAbsent(termValue, k -> new ArrayList<>())
                            .add(globalDocId);
                }
            }
        }
    }

    private long calculateIndexVersion(IndexReader left, IndexReader right) {
        return getVersion(left) + getVersion(right);
    }

    private long getVersion(IndexReader r) {
        if (r instanceof DirectoryReader dr) {
            return dr.getVersion();        // ✔ Only place where version exists
        }
        // No version available for non-DirectoryReader
        return 0L;
    }

    @Override
    public DocIdSet getMatchingDocs(IndexReader leftIndex, int docId, String fromField, String toField) throws IOException {
        if (!built) {
            throw new IllegalStateException("Index not built");
        }

        updateLastAccessTime();

        // Get field value using DocValues
        BytesRef fieldValue = getFieldValueWithDocValues(leftIndex, docId, fromField);
        if (fieldValue == null) {
            return null;
        }

        // Find matching documents in target field
        Map<BytesRef, List<Integer>> targetMap = fromField.equals(leftField) ? leftToRightMap : rightToLeftMap;
        List<Integer> matchingDocIds = targetMap.get(fieldValue);

        if (matchingDocIds == null || matchingDocIds.isEmpty()) {
            return null;
        }

        int[] ids = matchingDocIds.stream().mapToInt(Integer::intValue).toArray();
        return new IntArrayDocIdSet(ids, ids.length);
    }

    private BytesRef getFieldValueWithDocValues(IndexReader index, int docId, String field) throws IOException {
        for (LeafReaderContext context : index.leaves()) {
            if (docId >= context.docBase && docId < context.docBase + context.reader().maxDoc()) {
                int segmentDocId = docId - context.docBase;
                LeafReader reader = context.reader();
                SortedDocValues docValues = reader.getSortedDocValues(field);

                if (docValues != null && docValues.advanceExact(segmentDocId)) {
                    // DocValues reuse the same BytesRef; copy to make it stable for map lookup
                    return BytesRef.deepCopyOf(docValues.lookupOrd(docValues.ordValue()));
                }
                break;
            }
        }
        return null;
    }

    @Override
    public boolean isBuilt() {
        return built;
    }

    @Override
    public void clear() {
        leftToRightMap.clear();
        rightToLeftMap.clear();
        built = false;
        memoryUsage.set(0);
        indexVersion = -1;
    }

    @Override
    public long getMemoryUsage() {
        return memoryUsage.get();
    }

    /**
     * Lightweight DocIdSet backed by an int array. The iterator assumes doc IDs
     * are provided in ascending order.
     */
    private static final class IntArrayDocIdSet extends DocIdSet {
        private final int[] docIds;
        private final int length;

        IntArrayDocIdSet(int[] docIds, int length) {
            this.docIds = docIds;
            this.length = length;
        }

        @Override
        public DocIdSetIterator iterator() {
            if (length == 0) {
                return DocIdSetIterator.empty();
            }

            return new DocIdSetIterator() {
                private int idx = -1;
                private int current = -1;

                @Override
                public int docID() {
                    return current;
                }

                @Override
                public int nextDoc() {
                    idx++;
                    if (idx >= length) {
                        current = NO_MORE_DOCS;
                        return current;
                    }
                    current = docIds[idx];
                    return current;
                }

                @Override
                public int advance(int target) {
                    while (true) {
                        int doc = nextDoc();
                        if (doc >= target || doc == NO_MORE_DOCS) {
                            return doc;
                        }
                    }
                }

                @Override
                public long cost() {
                    return length;
                }
            };
        }

        @Override
        public long ramBytesUsed() {
            return Integer.BYTES * (long) length;
        }
    }

    private void calculateMemoryUsage() {
        long usage = 0;
        // Estimate map overhead + array sizes
        usage += leftToRightMap.size() * 40L; // approximate entry overhead
        usage += rightToLeftMap.size() * 40L;

        for (List<Integer> list : leftToRightMap.values()) {
            usage += list.size() * 4L; // 4 bytes per int
        }
        for (List<Integer> list : rightToLeftMap.values()) {
            usage += list.size() * 4L;
        }

        memoryUsage.set(usage);
    }

    private void updateLastAccessTime() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public JoinIndexType getType() {
        return JoinIndexType.IN_MEMORY;
    }
}