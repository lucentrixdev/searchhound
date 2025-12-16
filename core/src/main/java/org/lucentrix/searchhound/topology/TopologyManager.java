package org.lucentrix.searchhound.topology;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Manages cluster topology with heartbeat monitoring and dynamic updates
 */
public class TopologyManager {

    private final AtomicReference<ClusterTopology> topologyRef;
    private final ScheduledExecutorService heartbeatScheduler;
    private final Map<String, Long> nodeHeartbeats;
    private final long heartbeatIntervalMs = 5000; // 5 seconds
    private final long nodeTimeoutMs = 30000; // 30 seconds

    public TopologyManager(ClusterTopology initialTopology) {
        this.topologyRef = new AtomicReference<>(initialTopology);
        this.heartbeatScheduler = Executors.newScheduledThreadPool(1);
        this.nodeHeartbeats = new ConcurrentHashMap<>();

        startHeartbeatMonitoring();
    }

    private void startHeartbeatMonitoring() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                monitorNodeHealth();
                cleanupDeadNodes();
            } catch (Exception e) {
                System.err.println("Error in topology monitoring: " + e.getMessage());
            }
        }, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Simulate receiving heartbeats from nodes
     */
    public void receiveHeartbeat(String nodeId) {
        nodeHeartbeats.put(nodeId, System.currentTimeMillis());

        // Update node health score in topology
        ClusterTopology current = topologyRef.get();
        ClusterTopology.Node node = getNodeById(current, nodeId);
        if (node != null) {
            // In a real implementation, we would update the node's health score
            // based on recent heartbeats and other metrics
        }
    }

    private void monitorNodeHealth() {
        ClusterTopology current = topologyRef.get();
        long now = System.currentTimeMillis();

        for (ClusterTopology.Node node : current.getAllNodes()) {
            Long lastHeartbeat = nodeHeartbeats.get(node.getNodeId());
            boolean isHealthy = lastHeartbeat != null &&
                    (now - lastHeartbeat) < nodeTimeoutMs;

            // Update node health in topology if needed
            // In real implementation, we would create a new Node with updated health score
        }
    }

    private void cleanupDeadNodes() {
        ClusterTopology current = topologyRef.get();
        long now = System.currentTimeMillis();

        List<String> deadNodes = new ArrayList<>();
        for (Map.Entry<String, Long> entry : nodeHeartbeats.entrySet()) {
            if (now - entry.getValue() > nodeTimeoutMs * 2) {
                deadNodes.add(entry.getKey());
            }
        }

        if (!deadNodes.isEmpty()) {
            ClusterTopology newTopology = cloneTopology(current);
            for (String nodeId : deadNodes) {
                newTopology.removeNode(nodeId);
                nodeHeartbeats.remove(nodeId);
            }
            topologyRef.set(newTopology);

            System.out.println("Removed dead nodes: " + deadNodes);
        }
    }

    /**
     * Add a new node to the cluster
     */
    public void addNode(ClusterTopology.Node node) {
        ClusterTopology current = topologyRef.get();
        ClusterTopology newTopology = cloneTopology(current);
        newTopology.addNode(node);
        topologyRef.set(newTopology);

        // Record initial heartbeat
        nodeHeartbeats.put(node.getNodeId(), System.currentTimeMillis());

        System.out.println("Added new node: " + node);
    }

    /**
     * Remove a node from the cluster
     */
    public void removeNode(String nodeId) {
        ClusterTopology current = topologyRef.get();
        ClusterTopology newTopology = cloneTopology(current);
        newTopology.removeNode(nodeId);
        topologyRef.set(newTopology);

        nodeHeartbeats.remove(nodeId);

        System.out.println("Removed node: " + nodeId);
    }

    /**
     * Update node metadata
     */
    public void updateNodeMetadata(String nodeId, String key, Object value) {
        ClusterTopology current = topologyRef.get();
        ClusterTopology.Node node = getNodeById(current, nodeId);
        if (node != null) {
            // In real implementation, create updated node and replace in topology
        }
    }

    /**
     * Find optimal nodes for join execution
     */
    public List<String> findOptimalJoinNodes(String collection1, String collection2,
                                             JoinStrategy strategy) {
        ClusterTopology topology = topologyRef.get();

        List<ClusterTopology.Node> nodes1 = topology.getNodesHostingCollection(collection1);
        List<ClusterTopology.Node> nodes2 = topology.getNodesHostingCollection(collection2);

        if (nodes1.isEmpty() || nodes2.isEmpty()) {
            return Collections.emptyList();
        }

        switch (strategy) {
            case BROADCAST_HASH_JOIN:
                return planBroadcastHashJoin(nodes1, nodes2, topology);
            case DISTRIBUTED_HASH_JOIN:
                return planDistributedHashJoin(nodes1, nodes2, topology);
            case SORT_MERGE_JOIN:
                return planSortMergeJoin(nodes1, nodes2, topology);
            default:
                return nodes1.stream()
                        .map(ClusterTopology.Node::getNodeId)
                        .collect(Collectors.toList());
        }
    }

    /**
     * Plan sort-merge join execution across distributed nodes
     * Sort-merge join requires data to be sorted on join key before merging
     * This method identifies optimal nodes for sorting and merging phases
     */
    private List<String> planSortMergeJoin(List<ClusterTopology.Node> nodes1,
                                           List<ClusterTopology.Node> nodes2,
                                           ClusterTopology topology) {
        List<String> selectedNodes = new ArrayList<>();

        // Sort-merge join consists of two phases:
        // 1. Local sort phase on each node
        // 2. Merge phase across nodes

        // Strategy: Identify nodes that can efficiently sort and merge data
        // Prefer nodes in the same region to minimize network transfer during merge

        // Step 1: Group nodes by region
        Map<String, List<ClusterTopology.Node>> regionNodes1 = groupNodesByRegion(nodes1, topology);
        Map<String, List<ClusterTopology.Node>> regionNodes2 = groupNodesByRegion(nodes2, topology);

        // Step 2: Find common regions with nodes from both sides
        Set<String> commonRegions = findCommonRegions(regionNodes1, regionNodes2);

        if (!commonRegions.isEmpty()) {
            // Case 1: Nodes from both sides exist in common regions
            // Perform sort-merge within each region, then merge across regions

            for (String region : commonRegions) {
                List<ClusterTopology.Node> regionNodesForJoin =
                        selectRegionNodesForSortMerge(region, regionNodes1, regionNodes2, topology);

                regionNodesForJoin.stream()
                        .map(ClusterTopology.Node::getNodeId)
                        .forEach(selectedNodes::add);

                // Add region coordinator for merge phase
                ClusterTopology.Node regionCoordinator = topology.getRegionCoordinator(region);
                if (regionCoordinator != null && !selectedNodes.contains(regionCoordinator.getNodeId())) {
                    selectedNodes.add(regionCoordinator.getNodeId());
                }
            }

            // If data exists in multiple regions, we need a root coordinator for final merge
            if (commonRegions.size() > 1) {
                List<String> finalSelectedNodes = selectedNodes;
                topology.getRootCoordinators().stream()
                        .map(ClusterTopology.Node::getNodeId)
                        .filter(id -> !finalSelectedNodes.contains(id))
                        .findFirst()
                        .ifPresent(selectedNodes::add);
            }
        } else {
            // Case 2: No common regions - need cross-region sort-merge
            // This is more expensive due to network transfer

            selectedNodes = planCrossRegionSortMerge(regionNodes1, regionNodes2, topology);
        }

        // Step 3: Optimize node selection for sort capacity
        selectedNodes = prioritizeSortCapableNodes(selectedNodes, topology);

        // Step 4: Ensure we have enough nodes for parallel sorting
        selectedNodes = ensureMinimumNodes(selectedNodes, nodes1, nodes2, topology);

        return selectedNodes;
    }

    /**
     * Group nodes by region for better locality
     */
    private Map<String, List<ClusterTopology.Node>> groupNodesByRegion(
            List<ClusterTopology.Node> nodes, ClusterTopology topology) {
        Map<String, List<ClusterTopology.Node>> regionMap = new HashMap<>();

        for (ClusterTopology.Node node : nodes) {
            String regionId = node.getRegionId();
            regionMap.computeIfAbsent(regionId, k -> new ArrayList<>()).add(node);
        }

        return regionMap;
    }

    /**
     * Find regions that contain nodes from both join sides
     */
    private Set<String> findCommonRegions(
            Map<String, List<ClusterTopology.Node>> regionNodes1,
            Map<String, List<ClusterTopology.Node>> regionNodes2) {
        Set<String> commonRegions = new HashSet<>(regionNodes1.keySet());
        commonRegions.retainAll(regionNodes2.keySet());
        return commonRegions;
    }

    /**
     * Select optimal nodes within a region for sort-merge join
     */
    private List<ClusterTopology.Node> selectRegionNodesForSortMerge(
            String region,
            Map<String, List<ClusterTopology.Node>> regionNodes1,
            Map<String, List<ClusterTopology.Node>> regionNodes2,
            ClusterTopology topology) {

        List<ClusterTopology.Node> selectedNodes = new ArrayList<>();

        // Get all nodes in this region from both sides
        List<ClusterTopology.Node> regionNodesList1 = regionNodes1.getOrDefault(region, new ArrayList<>());
        List<ClusterTopology.Node> regionNodesList2 = regionNodes2.getOrDefault(region, new ArrayList<>());

        // Criteria for node selection:
        // 1. Health status
        // 2. Available memory (for sorting)
        // 3. CPU capacity
        // 4. Network proximity

        // Select healthy nodes with highest estimated sort capacity
        List<ClusterTopology.Node> allRegionNodes = new ArrayList<>();
        allRegionNodes.addAll(regionNodesList1);
        allRegionNodes.addAll(regionNodesList2);

        // Sort nodes by sort capacity (estimated)
        allRegionNodes.sort((n1, n2) -> {
            double capacity1 = estimateSortCapacity(n1);
            double capacity2 = estimateSortCapacity(n2);
            return Double.compare(capacity2, capacity1); // Descending
        });

        // Select top N nodes, but ensure we have at least one from each side
        int nodesToSelect = Math.min(
                Math.max(regionNodesList1.size(), regionNodesList2.size()),
                allRegionNodes.size()
        );

        // Ensure at least one node from each side is selected
        boolean hasSide1 = false;
        boolean hasSide2 = false;

        for (int i = 0; i < allRegionNodes.size() && selectedNodes.size() < nodesToSelect; i++) {
            ClusterTopology.Node node = allRegionNodes.get(i);

            if (node.isHealthy()) {
                // Track which side this node belongs to
                if (regionNodesList1.contains(node)) hasSide1 = true;
                if (regionNodesList2.contains(node)) hasSide2 = true;

                selectedNodes.add(node);
            }
        }

        // If missing nodes from one side, add them
        if (!hasSide1 && !regionNodesList1.isEmpty()) {
            regionNodesList1.stream()
                    .filter(ClusterTopology.Node::isHealthy)
                    .findFirst()
                    .ifPresent(selectedNodes::add);
        }

        if (!hasSide2 && !regionNodesList2.isEmpty()) {
            regionNodesList2.stream()
                    .filter(ClusterTopology.Node::isHealthy)
                    .findFirst()
                    .ifPresent(selectedNodes::add);
        }

        return selectedNodes;
    }

    /**
     * Plan sort-merge join when data is in different regions
     */
    private List<String> planCrossRegionSortMerge(
            Map<String, List<ClusterTopology.Node>> regionNodes1,
            Map<String, List<ClusterTopology.Node>> regionNodes2,
            ClusterTopology topology) {

        List<String> selectedNodes = new ArrayList<>();

        // Strategy for cross-region sort-merge:
        // 1. Choose a merge region (preferably with best connectivity)
        // 2. Select sort nodes from each region
        // 3. Transfer sorted data to merge region

        // Step 1: Find best merge region
        String mergeRegion = findBestMergeRegion(regionNodes1, regionNodes2, topology);

        // Step 2: Select sort nodes from each side
        List<ClusterTopology.Node> sortNodes = selectSortNodesForCrossRegion(
                regionNodes1, regionNodes2, mergeRegion, topology);

        // Step 3: Select merge nodes in the merge region
        List<ClusterTopology.Node> mergeNodes = selectMergeNodes(mergeRegion, topology);

        // Combine all selected nodes
        sortNodes.stream()
                .map(ClusterTopology.Node::getNodeId)
                .forEach(selectedNodes::add);

        mergeNodes.stream()
                .map(ClusterTopology.Node::getNodeId)
                .forEach(selectedNodes::add);

        // Add coordinators for orchestration
        ClusterTopology.Node regionCoordinator = topology.getRegionCoordinator(mergeRegion);
        if (regionCoordinator != null) {
            selectedNodes.add(regionCoordinator.getNodeId());
        }

        // Add root coordinator for overall coordination
        topology.getRootCoordinators().stream()
                .map(ClusterTopology.Node::getNodeId)
                .findFirst()
                .ifPresent(selectedNodes::add);

        return selectedNodes;
    }

    /**
     * Find the best region to serve as merge point for cross-region sort-merge
     */
    private String findBestMergeRegion(
            Map<String, List<ClusterTopology.Node>> regionNodes1,
            Map<String, List<ClusterTopology.Node>> regionNodes2,
            ClusterTopology topology) {

        // Simple heuristic: choose region with best connectivity to other regions
        // In production, use actual network latency measurements

        Set<String> allRegions = new HashSet<>();
        allRegions.addAll(regionNodes1.keySet());
        allRegions.addAll(regionNodes2.keySet());

        String bestRegion = null;
        double bestScore = Double.MAX_VALUE;

        for (String candidateRegion : allRegions) {
            // Calculate total network cost from candidate to all data regions
            double totalCost = 0.0;
            int regionCount = 0;

            for (String dataRegion : regionNodes1.keySet()) {
                if (!candidateRegion.equals(dataRegion)) {
                    totalCost += topology.calculateRegionNetworkCost(candidateRegion, dataRegion);
                    regionCount++;
                }
            }

            for (String dataRegion : regionNodes2.keySet()) {
                if (!candidateRegion.equals(dataRegion)) {
                    totalCost += topology.calculateRegionNetworkCost(candidateRegion, dataRegion);
                    regionCount++;
                }
            }

            double avgCost = regionCount > 0 ? totalCost / regionCount : 0;

            // Adjust score based on region tier (prefer higher tier for better performance)
            double tierFactor = getTierFactor(candidateRegion, topology);
            double score = avgCost * tierFactor;

            if (score < bestScore) {
                bestScore = score;
                bestRegion = candidateRegion;
            }
        }

        // Fallback: if no region found, use first region from side 1
        return bestRegion != null ? bestRegion :
                regionNodes1.keySet().stream().findFirst().orElse(null);
    }

    /**
     * Select nodes for sorting phase in cross-region scenario
     */
    private List<ClusterTopology.Node> selectSortNodesForCrossRegion(
            Map<String, List<ClusterTopology.Node>> regionNodes1,
            Map<String, List<ClusterTopology.Node>> regionNodes2,
            String mergeRegion,
            ClusterTopology topology) {

        List<ClusterTopology.Node> sortNodes = new ArrayList<>();

        // For each region, select the best node for sorting
        // Prefer nodes with high sort capacity and good connectivity to merge region

        // Process side 1 regions
        for (Map.Entry<String, List<ClusterTopology.Node>> entry : regionNodes1.entrySet()) {
            String region = entry.getKey();
            List<ClusterTopology.Node> nodes = entry.getValue();

            if (!region.equals(mergeRegion)) {
                // Select best sort node from this region
                ClusterTopology.Node bestNode = selectBestSortNode(nodes, mergeRegion, topology);
                if (bestNode != null) {
                    sortNodes.add(bestNode);
                }
            }
        }

        // Process side 2 regions
        for (Map.Entry<String, List<ClusterTopology.Node>> entry : regionNodes2.entrySet()) {
            String region = entry.getKey();
            List<ClusterTopology.Node> nodes = entry.getValue();

            if (!region.equals(mergeRegion)) {
                // Select best sort node from this region
                ClusterTopology.Node bestNode = selectBestSortNode(nodes, mergeRegion, topology);
                if (bestNode != null) {
                    sortNodes.add(bestNode);
                }
            }
        }

        return sortNodes;
    }

    /**
     * Select the best node for sorting from a list of nodes
     */
    private ClusterTopology.Node selectBestSortNode(List<ClusterTopology.Node> nodes,
                                                    String targetRegion,
                                                    ClusterTopology topology) {

        return nodes.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .max((n1, n2) -> {
                    // Compare nodes based on sort capacity and network cost to target
                    double score1 = calculateSortNodeScore(n1, targetRegion, topology);
                    double score2 = calculateSortNodeScore(n2, targetRegion, topology);
                    return Double.compare(score1, score2);
                })
                .orElse(null);
    }

    /**
     * Calculate score for a sort node (higher is better)
     */
    private double calculateSortNodeScore(ClusterTopology.Node node,
                                          String targetRegion,
                                          ClusterTopology topology) {

        // Factors:
        // 1. Sort capacity (memory, CPU)
        // 2. Network cost to target region (lower is better)
        // 3. Node health

        double sortCapacity = estimateSortCapacity(node);

        double networkCost = node.getRegionId().equals(targetRegion) ?
                1.0 : // Same region
                topology.calculateRegionNetworkCost(node.getRegionId(), targetRegion);

        double healthFactor = node.isHealthy() ? 1.0 : 0.0;

        // Normalize network cost (invert so lower cost = higher score)
        double normalizedNetwork = 1.0 / (1.0 + networkCost);

        // Combine factors with weights
        double capacityWeight = 0.6;
        double networkWeight = 0.3;
        double healthWeight = 0.1;

        return (sortCapacity * capacityWeight) +
                (normalizedNetwork * networkWeight) +
                (healthFactor * healthWeight);
    }

    /**
     * Select nodes for merge phase (in the merge region)
     */
    private List<ClusterTopology.Node> selectMergeNodes(String mergeRegion,
                                                        ClusterTopology topology) {

        List<ClusterTopology.Node> mergeNodes = new ArrayList<>();

        // Get all nodes in the merge region
        List<ClusterTopology.Node> regionNodes = topology.getNodesInRegion(mergeRegion);

        // Select nodes with good merge capacity (I/O throughput, memory)
        regionNodes.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .sorted((n1, n2) -> {
                    double mergeCap1 = estimateMergeCapacity(n1);
                    double mergeCap2 = estimateMergeCapacity(n2);
                    return Double.compare(mergeCap2, mergeCap1); // Descending
                })
                .limit(3) // Select up to 3 merge nodes
                .forEach(mergeNodes::add);

        return mergeNodes;
    }

    /**
     * Prioritize nodes with better sort capabilities
     */
    private List<String> prioritizeSortCapableNodes(List<String> nodeIds,
                                                    ClusterTopology topology) {

        // Convert to nodes and sort by sort capacity
        List<ClusterTopology.Node> nodes = new ArrayList<>();
        for (String nodeId : nodeIds) {
            topology.getAllNodes().stream()
                    .filter(n -> n.getNodeId().equals(nodeId))
                    .findFirst()
                    .ifPresent(nodes::add);
        }

        nodes.sort((n1, n2) -> {
            double cap1 = estimateSortCapacity(n1);
            double cap2 = estimateSortCapacity(n2);
            return Double.compare(cap2, cap1); // Descending
        });

        // Return node IDs, keeping only healthy nodes
        return nodes.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .map(ClusterTopology.Node::getNodeId)
                .collect(Collectors.toList());
    }

    /**
     * Ensure minimum number of nodes for parallel sorting
     */
    private List<String> ensureMinimumNodes(List<String> selectedNodes,
                                            List<ClusterTopology.Node> nodes1,
                                            List<ClusterTopology.Node> nodes2,
                                            ClusterTopology topology) {

        int minNodesRequired = calculateMinNodesRequired(nodes1, nodes2);

        if (selectedNodes.size() >= minNodesRequired) {
            return selectedNodes;
        }

        // Need to add more nodes
        List<String> additionalNodes = new ArrayList<>(selectedNodes);

        // Find additional healthy nodes with good sort capacity
        List<ClusterTopology.Node> allAvailableNodes = new ArrayList<>();
        allAvailableNodes.addAll(nodes1);
        allAvailableNodes.addAll(nodes2);

        // Remove already selected nodes
        allAvailableNodes.removeIf(node -> selectedNodes.contains(node.getNodeId()));

        // Sort by sort capacity and health
        allAvailableNodes.sort((n1, n2) -> {
            if (n1.isHealthy() != n2.isHealthy()) {
                return n1.isHealthy() ? -1 : 1; // Healthy first
            }
            double cap1 = estimateSortCapacity(n1);
            double cap2 = estimateSortCapacity(n2);
            return Double.compare(cap2, cap1); // Descending
        });

        // Add nodes until we reach minimum requirement
        for (ClusterTopology.Node node : allAvailableNodes) {
            if (additionalNodes.size() >= minNodesRequired) {
                break;
            }
            additionalNodes.add(node.getNodeId());
        }

        return additionalNodes;
    }

    /**
     * Calculate minimum nodes required for efficient sort-merge join
     */
    private int calculateMinNodesRequired(List<ClusterTopology.Node> nodes1,
                                          List<ClusterTopology.Node> nodes2) {

        // Simple heuristic based on data distribution
        // At minimum, we need nodes from both sides for sorting
        int minFromSide1 = Math.min(1, (int) nodes1.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .count());

        int minFromSide2 = Math.min(1, (int) nodes2.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .count());

        // Plus at least one merge node
        int minMergeNodes = 1;

        // Plus optionally coordinators (they'll be added separately)

        return Math.max(2, minFromSide1 + minFromSide2 + minMergeNodes);
    }

    /**
     * Estimate node's sort capacity (0.0 to 1.0)
     * Based on available memory, CPU cores, and disk I/O
     */
    private double estimateSortCapacity(ClusterTopology.Node node) {
        // In production, use actual metrics from node metadata
        // For now, use simple heuristics

        double capacity = 0.5; // Base capacity

        // Check metadata for capacity indicators
        Object memoryObj = node.getMetadata("availableMemory");
        Object cpuObj = node.getMetadata("cpuCores");
        Object diskObj = node.getMetadata("diskSpeed");

        if (memoryObj instanceof Long) {
            long memoryMB = (Long) memoryObj;
            if (memoryMB > 16384) capacity += 0.3; // >16GB
            else if (memoryMB > 8192) capacity += 0.2; // >8GB
            else if (memoryMB > 4096) capacity += 0.1; // >4GB
        }

        if (cpuObj instanceof Integer) {
            int cores = (Integer) cpuObj;
            if (cores > 8) capacity += 0.2;
            else if (cores > 4) capacity += 0.1;
        }

        // Adjust based on node role (SearchLeaf nodes are better for sorting)
        if (node.getRole() == ClusterTopology.NodeRole.SEARCH_LEAF) {
            capacity += 0.1;
        }

        // Adjust based on tier
        if (node.getTier() == ClusterTopology.DataCenterTier.TIER_1) {
            capacity += 0.1;
        }

        // Cap at 1.0
        return Math.min(1.0, capacity);
    }

    /**
     * Estimate node's merge capacity (for merging sorted streams)
     */
    private double estimateMergeCapacity(ClusterTopology.Node node) {
        // Merge phase is typically less resource-intensive than sort
        // Focus on network and I/O capabilities

        double capacity = 0.5; // Base capacity

        // Coordinators are good for merging
        if (node.getRole() == ClusterTopology.NodeRole.REGION_COORDINATOR ||
                node.getRole() == ClusterTopology.NodeRole.ZONE_COORDINATOR) {
            capacity += 0.3;
        }

        // Higher tier for better network
        if (node.getTier() == ClusterTopology.DataCenterTier.TIER_1) {
            capacity += 0.2;
        }

        return Math.min(1.0, capacity);
    }

    /**
     * Get tier factor for region selection
     */
    private double getTierFactor(String region, ClusterTopology topology) {
        // Get a representative node from the region to determine tier
        List<ClusterTopology.Node> regionNodes = topology.getNodesInRegion(region);
        if (!regionNodes.isEmpty()) {
            ClusterTopology.DataCenterTier tier = regionNodes.get(0).getTier();
            switch (tier) {
                case TIER_1: return 0.8; // Lower factor for better regions
                case TIER_2: return 1.0; // Standard
                case TIER_3: return 1.5; // Higher factor (penalty) for lower tiers
            }
        }
        return 1.0;
    }

    private List<String> planBroadcastHashJoin(List<ClusterTopology.Node> buildNodes,
                                               List<ClusterTopology.Node> probeNodes,
                                               ClusterTopology topology) {
        // Choose build side: smaller dataset, located in region with good connectivity
        // For simplicity, choose first healthy node from each list
        List<String> result = new ArrayList<>();

        buildNodes.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .findFirst()
                .ifPresent(node -> result.add(node.getNodeId()));

        probeNodes.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .limit(3) // Broadcast to 3 nodes
                .forEach(node -> result.add(node.getNodeId()));

        return result;
    }

    private List<String> planDistributedHashJoin(List<ClusterTopology.Node> nodes1,
                                                 List<ClusterTopology.Node> nodes2,
                                                 ClusterTopology topology) {
        // For distributed hash join, we need nodes from both sides
        List<String> result = new ArrayList<>();

        // Add healthy nodes from both sides, preferring nodes in the same region
        result.addAll(nodes1.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .map(ClusterTopology.Node::getNodeId)
                .limit(5)
                .toList());

        result.addAll(nodes2.stream()
                .filter(ClusterTopology.Node::isHealthy)
                .map(ClusterTopology.Node::getNodeId)
                .limit(5)
                .toList());

        return result;
    }

    private ClusterTopology cloneTopology(ClusterTopology original) {
        // In real implementation, create a deep copy
        // For now, return the same instance (simplified)
        return original;
    }

    private ClusterTopology.Node getNodeById(ClusterTopology topology, String nodeId) {
        return topology.getAllNodes().stream()
                .filter(node -> node.getNodeId().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    public void shutdown() {
        heartbeatScheduler.shutdown();
    }

    public enum JoinStrategy {
        BROADCAST_HASH_JOIN,
        DISTRIBUTED_HASH_JOIN,
        SORT_MERGE_JOIN
    }
}