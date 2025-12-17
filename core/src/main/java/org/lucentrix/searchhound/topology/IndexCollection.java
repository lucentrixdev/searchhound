package org.lucentrix.searchhound.topology;


import com.google.common.collect.Lists;
import org.lucentrix.searchhound.index.PlainLuceneIndex;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class IndexCollection {
    private final ExecutorService executor;
    private final String name;
    private final List<PlainLuceneIndex> nodes;

    public IndexCollection(String name, Collection<PlainLuceneIndex> nodes) {
        this.executor = Executors.newCachedThreadPool();
        this.name = name;
        this.nodes = nodes == null ? new ArrayList<>() : new ArrayList<>(nodes);
    }

    public IndexCollection(String name) {
        this(name, null);
    }

    // Basic sequential index
    //TODO make parallel indexing - we don't care about ingestion performance now
    public void indexDocuments(List<Map<String, Object>> docs) {
        for (List<Map<String, Object>> partition : Lists.partition(docs, 500)) {
            PlainLuceneIndex index = nodes.get(0);
            index.indexDocuments(partition);
            index.commit();
            //Sort by doc count
            nodes.sort(Comparator.comparingInt(PlainLuceneIndex::numDocs));
        }
    }

    public long numDocs() {
        long count = 0;
        for (PlainLuceneIndex node : nodes) {
            count += node.numDocs();
        }

        return count;
    }

    public List<Map<String, Object>> searchByJoinKeys(Collection<String> joinKeys) {
        List<Map<String, Object>> documents = new ArrayList<>();
        for (PlainLuceneIndex node : nodes) {
            documents.addAll(node.searchByJoinKeys(joinKeys));
        }

        return documents;
    }

    public Set<String> extractJoinKeys() {
        Set<Future<Set<String>>> buildTasks = new HashSet<>();
        for (PlainLuceneIndex node : nodes) {
            buildTasks.add(executor.submit(node::extractJoinKeys));
        }

        // Collect all join keys
        Set<String> allJoinKeys = new HashSet<>();
        for (Future<Set<String>> future : buildTasks) {
            try {
                allJoinKeys.addAll(future.get());
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return allJoinKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        IndexCollection that = (IndexCollection) o;
        return Objects.equals(name, that.name) && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    public void close() {
        executor.shutdownNow();

        for (PlainLuceneIndex index : nodes) {
            try {
                index.close();
            } catch (Exception ex) {
                //NOP
            }
        }
        nodes.clear();

        System.out.println("Index collection \""+this.name + "\" closed");
    }

    public String getName() {
        return name;
    }
}
