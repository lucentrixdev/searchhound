package org.lucentrix.searchhound;

import org.lucentrix.searchhound.core.DistributedJoinCoordinator;
import org.lucentrix.searchhound.index.PlainLuceneIndex;

import java.util.List;
import java.util.Map;

public class SearchHoundResearchApp {

    public static void main(String[] args) throws Exception {
        // Initialize services
        PlainLuceneIndex userService = new PlainLuceneIndex("data/indexes/users");
        PlainLuceneIndex orderService = new PlainLuceneIndex("data/indexes/orders");

        // Sample data indexing
        indexSampleData(userService, orderService);

        // Initialize coordinator
        DistributedJoinCoordinator coordinator = new DistributedJoinCoordinator();
        coordinator.registerNode("node1-user", userService);
        coordinator.registerNode("node2-order", orderService);

        // Execute distributed join
        List<Map<String, Object>> results = coordinator.executeDistributedJoin(
                "users", "orders", "user_id");

        System.out.println("Join results count: " + results.size());

        // Research metrics
        collectResearchMetrics(coordinator);

        userService.close();
        orderService.close();
    }

    private static void indexSampleData(PlainLuceneIndex userService,
                                        PlainLuceneIndex orderService) {
        // Index users
        Map<String, Object> user1 = Map.of(
                "user_id", "U001",
                "name", "Alice",
                "email", "alice@example.com"
        );
        userService.indexDocument("U001", user1, "user_id");

        // Index orders
        Map<String, Object> order1 = Map.of(
                "order_id", "O001",
                "user_id", "U001",
                "amount", 150.50
        );
        orderService.indexDocument("O001", order1, "user_id");
    }

    private static void collectResearchMetrics(DistributedJoinCoordinator coordinator) {
        // Collect metrics for research paper:
        // - Network transfer reduction with Bloom filters
        // - Join execution time
        // - Memory usage
        // - False positive rate impact
    }
}
