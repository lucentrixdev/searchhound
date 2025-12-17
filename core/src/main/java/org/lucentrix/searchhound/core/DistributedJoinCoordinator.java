package org.lucentrix.searchhound.core;

import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.lucentrix.searchhound.topology.IndexCollection;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class DistributedJoinCoordinator {
    private final ExecutorService executor;
    private final Map<String, IndexCollection> collections;

    public DistributedJoinCoordinator() {
        this.executor = Executors.newCachedThreadPool();
        this.collections = new ConcurrentHashMap<>();
    }

    /**
     * Execute distributed hash join between two collections
     */
    public List<Map<String, Object>> executeDistributedJoin(
            String leftCollection,
            String rightCollection,
            String joinKey) {

        // Step 1: Choose build and probe sides based on cardinality
        String buildSide = chooseBuildSide(leftCollection, rightCollection);
        String probeSide = buildSide.equals(leftCollection) ? rightCollection : leftCollection;

        IndexCollection buildCollection = getCollection(buildSide);

        IndexCollection probeCollection = getCollection(probeSide);

        Set<String> allJoinKeys = buildCollection.extractJoinKeys();

        List<Map<String, Object>> partialResults = new ArrayList<>();
        for(List<String> joinKeyPartition : Lists.partition(new ArrayList<>(allJoinKeys), 1000)) {
            partialResults.addAll(buildCollection.searchByJoinKeys(joinKeyPartition));
            partialResults.addAll(probeCollection.searchByJoinKeys(joinKeyPartition));
        }

        // Step 6: Perform final join semantics
        return performFinalJoin(partialResults, buildSide, probeSide, joinKey);
    }

    private String chooseBuildSide(String left, String right) {
        // Simple heuristic: choose smaller collection as build side
        // In production, use cardinality estimates from statistics
        long leftSize = estimateCollectionSize(left);
        long rightSize = estimateCollectionSize(right);

        return leftSize <= rightSize ? left : right;
    }

    private long estimateCollectionSize(String collection) {
        // Estimate based on sampling or statistics
        IndexCollection index = collections.get(collection);
        if (index == null) {
            throw new RuntimeException("Index node is not found: " + collection);
        }
        return index.numDocs();
    }

    private IndexCollection getCollection(String name) {

        return collections.get(name);
    }

    private Map<String, Object> documentToMap(Document doc) {
        Map<String, Object> map = new HashMap<>();
        for (IndexableField field : doc.getFields()) {
            map.put(field.name(), doc.get(field.name()));
        }
        return map;
    }

    private List<Map<String, Object>> performFinalJoin(
            List<Map<String, Object>> partialResults,
            String buildSide,
            String probeSide,
            String joinKey) {
        // Implement final join logic (INNER, LEFT, etc.)
        // LEFT join by default
        return partialResults.stream().filter(map -> buildSide.equals(map.get("class_name"))).toList();
    }

    public void register(IndexCollection collection) {
        collections.put(collection.getName(), collection);
    }

    public void close() {
        for (IndexCollection collection : collections.values()) {
            try {
                collection.close();
            } catch (Exception ex) {
                //NOP
            }
        }
        collections.clear();

        executor.shutdownNow();
    }
}
