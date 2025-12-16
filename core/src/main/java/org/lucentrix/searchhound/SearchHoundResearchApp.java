package org.lucentrix.searchhound;

import org.apache.commons.io.FileUtils;
import org.lucentrix.searchhound.core.DistributedJoinCoordinator;
import org.lucentrix.searchhound.index.PlainLuceneIndex;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SearchHoundResearchApp {

    public static void main(String[] args) throws Exception {
        File indicesFolder = new File("data");
        if(indicesFolder.exists()) {
            try {
                FileUtils.deleteDirectory(indicesFolder);
                System.out.println("Directory and its contents deleted using Commons IO.");
            } catch (IOException e) {
                System.err.println("Failed to delete directory: " + e.getMessage());
            }
        }
        // Initialize services
        PlainLuceneIndex userService = new PlainLuceneIndex("data/indexes/users");
        PlainLuceneIndex orderService = new PlainLuceneIndex("data/indexes/orders");

        // Sample data indexing
        indexSampleData(userService, orderService, 10);

        // Initialize coordinator
        DistributedJoinCoordinator coordinator = new DistributedJoinCoordinator();
        coordinator.registerNode("user", userService);
        coordinator.registerNode("order", orderService);

        // Execute distributed join
        List<Map<String, Object>> results = coordinator.executeDistributedJoin(
                "user", "order", "user_id");

        System.out.println("Join results count: " + results.size());

        // Research metrics
        collectResearchMetrics(coordinator);

        coordinator.close();
    }

    private static void indexSampleData(PlainLuceneIndex userService, PlainLuceneIndex orderService, int count) {
        try {
            for (int i = 0; i < count; i++) {
                // Index users
                String userId = "U-"+ UUID.randomUUID();
                String orderId = "O-"+ UUID.randomUUID();

                Map<String, Object> user = Map.of(
                        "user_id", userId,
                        "name", "Alice",
                        "email", "alice@example.com",
                        "class_name", "user"
                );
                userService.indexDocument(userId, user, "user_id");


                // Index orders
                Map<String, Object> order = Map.of(
                        "order_id", orderId,
                        "user_id", userId,
                        "amount", 150.50,
                        "class_name", "order"
                );
                orderService.indexDocument(orderId, order, "user_id");
            }
        } finally {
            userService.commit();
            orderService.commit();
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
