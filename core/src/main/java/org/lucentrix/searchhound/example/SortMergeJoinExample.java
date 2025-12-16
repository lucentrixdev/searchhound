package org.lucentrix.searchhound.example;

import org.lucentrix.searchhound.topology.ClusterTopology;
import org.lucentrix.searchhound.topology.ClusterTopologyBuilder;
import org.lucentrix.searchhound.topology.SortMergeJoinStrategy;
import org.lucentrix.searchhound.topology.TopologyManager;

import java.util.List;

public class SortMergeJoinExample {

    public static void main(String[] args) {
        // Build test topology
        ClusterTopology topology = new ClusterTopologyBuilder("root-1")
                .buildTestTopology()
                .build();

        // Create topology manager
        TopologyManager manager = new TopologyManager(topology);

        // Find optimal nodes for sort-merge join
        List<String> sortMergeNodes = manager.findOptimalJoinNodes(
                "users", "orders", TopologyManager.JoinStrategy.SORT_MERGE_JOIN);

        System.out.println("=== SORT-MERGE JOIN NODES ===");
        System.out.println("Selected nodes: " + sortMergeNodes.size());
        sortMergeNodes.forEach(nodeId -> {
            ClusterTopology.Node node = findNodeById(topology, nodeId);
            System.out.println("  - " + nodeId +
                    " (Role: " + node.getRole() +
                    ", Region: " + node.getRegionId() +
                    ", Healthy: " + node.isHealthy() + ")");
        });

        // Create detailed sort-merge join plan
        SortMergeJoinStrategy.SortMergeJoinPlan detailedPlan =
                SortMergeJoinStrategy.createDetailedPlan(topology, "users", "orders", "user_id");

        System.out.println("\n=== DETAILED SORT-MERGE JOIN PLAN ===");
        System.out.println("Sort nodes: " + detailedPlan.getSortNodes().size());
        System.out.println("Merge nodes: " + detailedPlan.getMergeNodes().size());
        System.out.println("Coordinators: " + detailedPlan.getCoordinators().size());
        System.out.println("Estimated cost: " + detailedPlan.getEstimatedCost());
        System.out.println("Estimated data transfer: " +
                formatBytes(detailedPlan.getEstimatedDataTransfer()));

        // Simulate join execution phases
        System.out.println("\n=== EXECUTION PHASES ===");
        for (SortMergeJoinStrategy.MergePhase phase : SortMergeJoinStrategy.MergePhase.values()) {
            List<String> phaseNodes = detailedPlan.getPhaseNodes().get(phase);
            System.out.println(phase + ": " + (phaseNodes == null ? 0 : phaseNodes.size()) + " nodes");
        }
    }

    private static ClusterTopology.Node findNodeById(ClusterTopology topology, String nodeId) {
        return topology.getAllNodes().stream()
                .filter(n -> n.getNodeId().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
