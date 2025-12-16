package org.lucentrix.searchhound.topology;


import java.util.*;

/**
 * Implements distributed sort-merge join strategy
 */
public class SortMergeJoinStrategy {

    public enum MergePhase {
        LOCAL_SORT,      // Sort data locally on each node
        REGION_MERGE,    // Merge sorted streams within region
        GLOBAL_MERGE,    // Merge across regions
        FINAL_JOIN       // Perform actual join on merged streams
    }

    /**
     * Plan sort-merge join execution
     */
    public static class SortMergeJoinPlan {
        private final List<String> sortNodes;
        private final List<String> mergeNodes;
        private final List<String> coordinators;
        private final Map<MergePhase, List<String>> phaseNodes;
        private final double estimatedCost;
        private final long estimatedDataTransfer;

        public SortMergeJoinPlan(List<String> sortNodes, List<String> mergeNodes,
                                 List<String> coordinators, double estimatedCost,
                                 long estimatedDataTransfer) {
            this.sortNodes = sortNodes;
            this.mergeNodes = mergeNodes;
            this.coordinators = coordinators;
            this.estimatedCost = estimatedCost;
            this.estimatedDataTransfer = estimatedDataTransfer;

            this.phaseNodes = new HashMap<>();
            phaseNodes.put(MergePhase.LOCAL_SORT, sortNodes);
            phaseNodes.put(MergePhase.REGION_MERGE, mergeNodes);
            phaseNodes.put(MergePhase.GLOBAL_MERGE, coordinators);
        }

        // Getters
        public List<String> getSortNodes() { return sortNodes; }
        public List<String> getMergeNodes() { return mergeNodes; }
        public List<String> getCoordinators() { return coordinators; }
        public Map<MergePhase, List<String>> getPhaseNodes() { return phaseNodes; }
        public double getEstimatedCost() { return estimatedCost; }
        public long getEstimatedDataTransfer() { return estimatedDataTransfer; }

        public int getTotalNodes() {
            Set<String> allNodes = new HashSet<>();
            allNodes.addAll(sortNodes);
            allNodes.addAll(mergeNodes);
            allNodes.addAll(coordinators);
            return allNodes.size();
        }
    }

    /**
     * Create detailed sort-merge join plan based on topology
     */
    public static SortMergeJoinPlan createDetailedPlan(
            ClusterTopology topology,
            String collection1,
            String collection2,
            String joinKey) {

        // Get nodes hosting the collections
        List<ClusterTopology.Node> nodes1 = topology.getNodesHostingCollection(collection1);
        List<ClusterTopology.Node> nodes2 = topology.getNodesHostingCollection(collection2);

        // Use TopologyManager to get optimal nodes
        TopologyManager manager = new TopologyManager(topology);
        List<String> allNodes = manager.findOptimalJoinNodes(collection1, collection2,
                TopologyManager.JoinStrategy.SORT_MERGE_JOIN);

        // Categorize nodes by their roles
        List<String> sortNodes = new ArrayList<>();
        List<String> mergeNodes = new ArrayList<>();
        List<String> coordinators = new ArrayList<>();

        for (String nodeId : allNodes) {
            ClusterTopology.Node node = findNodeById(topology, nodeId);
            if (node != null) {
                switch (node.getRole()) {
                    case SEARCH_LEAF:
                        sortNodes.add(nodeId);
                        break;
                    case REGION_COORDINATOR:
                    case ZONE_COORDINATOR:
                        mergeNodes.add(nodeId);
                        break;
                    case ROOT_COORDINATOR:
                        coordinators.add(nodeId);
                        break;
                    default:
                        // For other roles, decide based on capabilities
                        if (isGoodForSorting(node)) {
                            sortNodes.add(nodeId);
                        } else {
                            mergeNodes.add(nodeId);
                        }
                }
            }
        }

        // Estimate costs
        double estimatedCost = estimateSortMergeCost(topology, nodes1, nodes2,
                sortNodes, mergeNodes, coordinators);

        long estimatedTransfer = estimateDataTransfer(topology, nodes1, nodes2,
                sortNodes, mergeNodes);

        return new SortMergeJoinPlan(sortNodes, mergeNodes, coordinators,
                estimatedCost, estimatedTransfer);
    }

    /**
     * Estimate cost of sort-merge join
     */
    private static double estimateSortMergeCost(ClusterTopology topology,
                                                List<ClusterTopology.Node> nodes1,
                                                List<ClusterTopology.Node> nodes2,
                                                List<String> sortNodes,
                                                List<String> mergeNodes,
                                                List<String> coordinators) {

        // Cost factors:
        // 1. Sorting cost (CPU, memory)
        // 2. Network transfer cost
        // 3. Merging cost

        double sortingCost = estimateSortingCost(nodes1, nodes2);
        double transferCost = estimateTransferCost(topology, nodes1, nodes2, sortNodes, mergeNodes);
        double mergingCost = estimateMergingCost(mergeNodes, coordinators);

        // Weighted sum
        return (sortingCost * 0.4) + (transferCost * 0.4) + (mergingCost * 0.2);
    }

    private static double estimateSortingCost(List<ClusterTopology.Node> nodes1,
                                              List<ClusterTopology.Node> nodes2) {
        // Estimate based on data size and number of sort nodes
        long totalDocs = nodes1.stream()
                .mapToLong(n -> n.getCollectionDocumentCount("collection1"))
                .sum() +
                nodes2.stream()
                        .mapToLong(n -> n.getCollectionDocumentCount("collection2"))
                        .sum();

        // O(n log n) complexity approximation
        return totalDocs * Math.log(totalDocs) / 1000000.0;
    }

    private static double estimateTransferCost(ClusterTopology topology,
                                               List<ClusterTopology.Node> nodes1,
                                               List<ClusterTopology.Node> nodes2,
                                               List<String> sortNodes,
                                               List<String> mergeNodes) {

        // Simplified: assume data moves from source nodes to sort/merge nodes
        // In reality, this would be more complex

        long totalDataSize = estimateTotalDataSize(nodes1, nodes2);

        // Estimate average network cost
        double avgNetworkCost = 1.0; // Base cost

        // Adjust based on topology
        if (!areNodesInSameRegion(sortNodes, mergeNodes, topology)) {
            avgNetworkCost *= 2.0; // Cross-region is more expensive
        }

        return totalDataSize * avgNetworkCost / 1000000.0;
    }

    private static double estimateMergingCost(List<String> mergeNodes,
                                              List<String> coordinators) {
        // Merging is typically O(n) for sorted streams
        int totalMergeNodes = mergeNodes.size() + coordinators.size();
        return totalMergeNodes * 100.0; // Base cost per merge node
    }

    private static long estimateDataTransfer(ClusterTopology topology,
                                             List<ClusterTopology.Node> nodes1,
                                             List<ClusterTopology.Node> nodes2,
                                             List<String> sortNodes,
                                             List<String> mergeNodes) {

        // Estimate total data transfer in bytes
        long dataSize1 = estimateCollectionDataSize(nodes1);
        long dataSize2 = estimateCollectionDataSize(nodes2);

        // For sort-merge join, data typically needs to be transferred:
        // 1. To sort nodes (if not already there)
        // 2. From sort nodes to merge nodes

        // Simplified: assume all data moves at least once
        return dataSize1 + dataSize2;
    }

    // Helper methods
    private static ClusterTopology.Node findNodeById(ClusterTopology topology, String nodeId) {
        return topology.getAllNodes().stream()
                .filter(n -> n.getNodeId().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    private static boolean isGoodForSorting(ClusterTopology.Node node) {
        // Check if node has characteristics good for sorting
        if (node.getRole() == ClusterTopology.NodeRole.SEARCH_LEAF) {
            return true;
        }

        // Check metadata for sorting capability indicators
        Object memory = node.getMetadata("availableMemory");
        if (memory instanceof Long && (Long) memory > 4096) {
            return true; // More than 4GB
        }

        return false;
    }

    private static boolean areNodesInSameRegion(List<String> nodes1,
                                                List<String> nodes2,
                                                ClusterTopology topology) {
        if (nodes1.isEmpty() || nodes2.isEmpty()) {
            return false;
        }

        // Get region of first node in each list
        String region1 = topology.getZoneForNode(nodes1.get(0));
        String region2 = topology.getZoneForNode(nodes2.get(0));

        return region1 != null && region1.equals(region2);
    }

    private static long estimateTotalDataSize(List<ClusterTopology.Node> nodes1,
                                              List<ClusterTopology.Node> nodes2) {
        // Simple estimation: assume average document size
        long docs1 = nodes1.stream()
                .mapToLong(n -> n.getCollectionDocumentCount("collection1"))
                .sum();

        long docs2 = nodes2.stream()
                .mapToLong(n -> n.getCollectionDocumentCount("collection2"))
                .sum();

        long avgDocSize = 1024; // 1KB per document (simplified)

        return (docs1 + docs2) * avgDocSize;
    }

    private static long estimateCollectionDataSize(List<ClusterTopology.Node> nodes) {
        long totalDocs = nodes.stream()
                .mapToLong(n -> n.getCollectionDocumentCount("collection"))
                .sum();

        long avgDocSize = 1024; // 1KB per document
        return totalDocs * avgDocSize;
    }
}