package org.lucentrix.searchhound.core;

import com.google.common.hash.BloomFilter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.document.Document;
import org.lucentrix.searchhound.index.PlainLuceneIndex;

import java.util.*;
import java.util.concurrent.*;


public class DistributedJoinCoordinator {
    private final ExecutorService executor;
    private final Map<String, PlainLuceneIndex> nodeServices;

    public DistributedJoinCoordinator() {
        this.executor = Executors.newCachedThreadPool();
        this.nodeServices = new ConcurrentHashMap<>();
    }

    /**
     * Execute distributed hash join between two collections
     */
    public List<Map<String, Object>> executeDistributedJoin(
            String leftCollection,
            String rightCollection,
            String joinKey) throws Exception {

        // Step 1: Choose build and probe sides based on cardinality
        String buildSide = chooseBuildSide(leftCollection, rightCollection);
        String probeSide = buildSide.equals(leftCollection) ? rightCollection : leftCollection;

        // Step 2: Build phase - extract join keys from build side
        Set<Future<Set<String>>> buildTasks = new HashSet<>();
        for (PlainLuceneIndex service : getServicesForCollection(buildSide)) {
            buildTasks.add(executor.submit(() -> service.extractJoinKeys(joinKey)));
        }

        // Collect all join keys
        Set<String> allJoinKeys = new HashSet<>();
        for (Future<Set<String>> future : buildTasks) {
            allJoinKeys.addAll(future.get());
        }

        // Step 3: Create and broadcast Bloom filter
        BloomFilter<String> bloomFilter = BloomFilterService.createFilter(allJoinKeys, 0.01);

        // Step 4: Probe phase - parallel execution on probe side
        List<Callable<List<Document>>> probeTasks = new ArrayList<>();
        for (PlainLuceneIndex service : getServicesForCollection(probeSide)) {
            probeTasks.add(() -> {
                // For each join key, search and return matching documents
                List<Document> results = new ArrayList<>();
                for (String key : allJoinKeys) {
                    results.addAll(service.searchByJoinKey(key, bloomFilter));
                }
                return results;
            });
        }

        // Step 5: Collect and merge results
        List<Map<String, Object>> finalResults = new ArrayList<>();
        for (Future<List<Document>> future : executor.invokeAll(probeTasks)) {
            for (Document doc : future.get()) {
                finalResults.add(documentToMap(doc));
            }
        }

        // Step 6: Perform final join semantics
        return performFinalJoin(finalResults, buildSide, probeSide, joinKey);
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
        return 0; // Placeholder
    }

    private List<PlainLuceneIndex> getServicesForCollection(String collection) {
        // Return all services hosting this collection
        return new ArrayList<>(nodeServices.values()); // Simplified
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
        return partialResults;
    }

    public void registerNode(String nodeId, PlainLuceneIndex service) {
        nodeServices.put(nodeId, service);
    }
}
