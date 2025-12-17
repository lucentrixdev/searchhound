package org.lucentrix.searchhound;

import org.apache.commons.io.FileUtils;
import org.lucentrix.searchhound.core.DistributedJoinCoordinator;
import org.lucentrix.searchhound.index.PlainLuceneIndex;
import org.lucentrix.searchhound.topology.IndexCollection;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SearchHoundResearchApp {
    static  final Random RND = new Random();

    private static void cleanup() {
        File indicesFolder = new File("data");
        if(indicesFolder.exists()) {
            try {
                FileUtils.deleteDirectory(indicesFolder);
                System.out.println("Directory "+indicesFolder+" and its contents deleted using Commons IO.");
            } catch (IOException e) {
                System.err.println("Failed to delete directory: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        cleanup();

        try {
            int maxNodeSize = 100000;
            // Initialize services
            IndexCollection userCollection = new IndexCollection("user", List.of(
                    new PlainLuceneIndex(Path.of("data/indexes/users1"), "user_id", maxNodeSize),
                    new PlainLuceneIndex(Path.of("data/indexes/users2"), "user_id", maxNodeSize),
                    new PlainLuceneIndex(Path.of("data/indexes/users3"), "user_id", maxNodeSize)
            ));

            IndexCollection orderCollection = new IndexCollection("order", List.of(
                    new PlainLuceneIndex(Path.of("data/indexes/orders1"), "user_id", maxNodeSize),
                    new PlainLuceneIndex(Path.of("data/indexes/orders2"), "user_id", maxNodeSize),
                    new PlainLuceneIndex(Path.of("data/indexes/orders3"), "user_id", maxNodeSize)
            ));


            // Sample data indexing
            indexSampleData(userCollection, orderCollection, 10000);

            // Initialize coordinator
            DistributedJoinCoordinator coordinator = new DistributedJoinCoordinator();
            coordinator.register(userCollection);
            coordinator.register(orderCollection);

            // Execute distributed join
            List<Map<String, Object>> results = coordinator.executeDistributedJoin(
                    "user", "order", "user_id");

            System.out.println("Join results count: " + results.size());

            // Research metrics
            collectResearchMetrics(coordinator);

            coordinator.close();
        } finally {
            cleanup();
        }

        System.out.println("Search demo completed");
    }

    private static void indexSampleData(IndexCollection userCollection, IndexCollection orderCollection, int count) {
        List<Map<String, Object>> users = new ArrayList<>();
        List<Map<String, Object>> orders = new ArrayList<>();

        try {
            for (int i = 0; i < count; i++) {
                String userId = "U-"+ RND.nextLong();
                String orderId = "O-"+ RND.nextLong();

                users.add(Map.of(
                        "id", userId,
                        "user_id", userId,
                        "name", "Alice",
                        "email", "alice@example.com",
                        "class_name", "user"
                ));
                orders.add(Map.of(
                        "id", orderId,
                        "order_id", orderId,
                        "user_id", userId,
                        "amount", 150.50,
                        "class_name", "order"
                ));

                if(i%1000 == 0) {
                    userCollection.indexDocuments(users);
                    users.clear();

                    orderCollection.indexDocuments(orders);
                    orders.clear();
                }
            }
        } finally {
            userCollection.indexDocuments(users);
            users.clear();

            orderCollection.indexDocuments(orders);
            orders.clear();
        }
    }

    private static void collectResearchMetrics(DistributedJoinCoordinator coordinator) {
        // Collect metrics for research paper:
        // - Network transfer reduction with Bloom filters
        // - Join execution time
        // - Memory usage
        // - False positive rate impact
    }
}
