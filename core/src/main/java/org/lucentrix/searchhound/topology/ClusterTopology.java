package org.lucentrix.searchhound.topology;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Models the hierarchical cluster topology for SearchHound distributed joins
 * Root → Regions → Zones → Nodes (SearchLeaf)
 */
public class ClusterTopology {

    public enum NodeRole {
        ROOT_COORDINATOR,
        REGION_COORDINATOR,
        ZONE_COORDINATOR,
        SEARCH_LEAF,
        BLOOM_FILTER_SERVICE,
        JOIN_COORDINATOR
    }

    public enum DataCenterTier {
        TIER_1, // Premium, low-latency regions
        TIER_2, // Standard regions
        TIER_3  // Cost-optimized regions
    }

    public static class Node {
        private final String nodeId;
        private final String host;
        private final int port;
        private final NodeRole role;
        private final DataCenterTier tier;
        private final String zoneId;
        private final String regionId;
        private final Map<String, Object> metadata;
        private final double healthScore; // 0.0 to 1.0
        private final long lastHeartbeat;
        private final Set<String> hostedCollections;
        private final Map<String, Long> collectionStats; // collection -> document count

        public Node(String nodeId, String host, int port, NodeRole role,
                    DataCenterTier tier, String zoneId, String regionId) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.role = role;
            this.tier = tier;
            this.zoneId = zoneId;
            this.regionId = regionId;
            this.metadata = new ConcurrentHashMap<>();
            this.healthScore = 1.0;
            this.lastHeartbeat = System.currentTimeMillis();
            this.hostedCollections = ConcurrentHashMap.newKeySet();
            this.collectionStats = new ConcurrentHashMap<>();
        }

        // Getters
        public String getNodeId() { return nodeId; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public NodeRole getRole() { return role; }
        public DataCenterTier getTier() { return tier; }
        public String getZoneId() { return zoneId; }
        public String getRegionId() { return regionId; }
        public String getAddress() { return host + ":" + port; }
        public double getHealthScore() { return healthScore; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public Set<String> getHostedCollections() { return new HashSet<>(hostedCollections); }
        public Map<String, Long> getCollectionStats() { return new HashMap<>(collectionStats); }

        // Metadata management
        public void putMetadata(String key, Object value) {
            metadata.put(key, value);
        }

        public Object getMetadata(String key) {
            return metadata.get(key);
        }

        public boolean isHealthy() {
            return healthScore > 0.7 &&
                    (System.currentTimeMillis() - lastHeartbeat) < 30000; // 30 second threshold
        }

        public void addCollection(String collection, long documentCount) {
            hostedCollections.add(collection);
            collectionStats.put(collection, documentCount);
        }

        public void removeCollection(String collection) {
            hostedCollections.remove(collection);
            collectionStats.remove(collection);
        }

        public boolean hostsCollection(String collection) {
            return hostedCollections.contains(collection);
        }

        public long getCollectionDocumentCount(String collection) {
            return collectionStats.getOrDefault(collection, 0L);
        }

        @Override
        public String toString() {
            return String.format("Node{id=%s, role=%s, zone=%s, region=%s, healthy=%s, collections=%s}",
                    nodeId, role, zoneId, regionId, isHealthy(), hostedCollections.size());
        }
    }

    // Topology structure
    private final Map<String, Node> allNodes = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> regionToZones = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> zoneToNodes = new ConcurrentHashMap<>();
    private final Map<String, String> nodeToZone = new ConcurrentHashMap<>();
    private final Map<String, String> zoneToRegion = new ConcurrentHashMap<>();

    private final String rootNodeId;
    private final Map<String, Node> rootNodes = new ConcurrentHashMap<>();
    private final Map<String, Node> regionCoordinators = new ConcurrentHashMap<>();
    private final Map<String, Node> zoneCoordinators = new ConcurrentHashMap<>();

    // Network latency matrix (simplified - in production use real measurements)
    private final Map<String, Map<String, Integer>> latencyMatrix = new ConcurrentHashMap<>();

    // Collection placement tracking
    private final Map<String, Set<String>> collectionToNodes = new ConcurrentHashMap<>();
    private final Map<String, String> collectionToPrimaryRegion = new ConcurrentHashMap<>();

    public ClusterTopology(String rootNodeId) {
        this.rootNodeId = rootNodeId;
    }

    /**
     * Add a node to the topology
     */
    public synchronized void addNode(Node node) {
        allNodes.put(node.getNodeId(), node);
        nodeToZone.put(node.getNodeId(), node.getZoneId());

        // Update region-zone mapping
        regionToZones.computeIfAbsent(node.getRegionId(), k -> ConcurrentHashMap.newKeySet())
                .add(node.getZoneId());

        // Update zone-node mapping
        zoneToNodes.computeIfAbsent(node.getZoneId(), k -> ConcurrentHashMap.newKeySet())
                .add(node.getNodeId());

        // Update zone-region mapping
        zoneToRegion.put(node.getZoneId(), node.getRegionId());

        // Categorize by role
        switch (node.getRole()) {
            case ROOT_COORDINATOR:
                rootNodes.put(node.getNodeId(), node);
                break;
            case REGION_COORDINATOR:
                regionCoordinators.put(node.getRegionId(), node);
                break;
            case ZONE_COORDINATOR:
                zoneCoordinators.put(node.getZoneId(), node);
                break;
        }

        // Update collection placement tracking
        for (String collection : node.getHostedCollections()) {
            collectionToNodes.computeIfAbsent(collection, k -> ConcurrentHashMap.newKeySet())
                    .add(node.getNodeId());
        }
    }

    /**
     * Remove a node from topology (e.g., node failure)
     */
    public synchronized void removeNode(String nodeId) {
        Node node = allNodes.remove(nodeId);
        if (node != null) {
            String zoneId = node.getZoneId();
            String regionId = node.getRegionId();

            // Remove from zone
            Set<String> zoneNodes = zoneToNodes.get(zoneId);
            if (zoneNodes != null) {
                zoneNodes.remove(nodeId);
                if (zoneNodes.isEmpty()) {
                    zoneToNodes.remove(zoneId);
                    zoneToRegion.remove(zoneId);

                    // Remove zone from region
                    Set<String> regionZones = regionToZones.get(regionId);
                    if (regionZones != null) {
                        regionZones.remove(zoneId);
                        if (regionZones.isEmpty()) {
                            regionToZones.remove(regionId);
                        }
                    }
                }
            }

            nodeToZone.remove(nodeId);

            // Remove from role-specific maps
            switch (node.getRole()) {
                case ROOT_COORDINATOR:
                    rootNodes.remove(nodeId);
                    break;
                case REGION_COORDINATOR:
                    regionCoordinators.remove(regionId);
                    break;
                case ZONE_COORDINATOR:
                    zoneCoordinators.remove(zoneId);
                    break;
            }

            // Remove from collection placement
            for (String collection : node.getHostedCollections()) {
                Set<String> nodes = collectionToNodes.get(collection);
                if (nodes != null) {
                    nodes.remove(nodeId);
                    if (nodes.isEmpty()) {
                        collectionToNodes.remove(collection);
                    }
                }
            }
        }
    }

    /**
     * Get all nodes in the cluster
     */
    public Collection<Node> getAllNodes() {
        return new ArrayList<>(allNodes.values());
    }

    /**
     * Get all nodes with a specific role
     */
    public List<Node> getNodesByRole(NodeRole role) {
        return allNodes.values().stream()
                .filter(node -> node.getRole() == role)
                .collect(Collectors.toList());
    }

    /**
     * Get all SearchLeaf nodes
     */
    public List<Node> getSearchLeafNodes() {
        return getNodesByRole(NodeRole.SEARCH_LEAF);
    }

    /**
     * Get nodes in a specific region
     */
    public List<Node> getNodesInRegion(String regionId) {
        return allNodes.values().stream()
                .filter(node -> node.getRegionId().equals(regionId))
                .collect(Collectors.toList());
    }

    /**
     * Get nodes in a specific zone
     */
    public List<Node> getNodesInZone(String zoneId) {
        return allNodes.values().stream()
                .filter(node -> node.getZoneId().equals(zoneId))
                .collect(Collectors.toList());
    }

    /**
     * Get nodes hosting a specific collection
     */
    public List<Node> getNodesHostingCollection(String collection) {
        Set<String> nodeIds = collectionToNodes.get(collection);
        if (nodeIds == null) {
            return Collections.emptyList();
        }

        return nodeIds.stream()
                .map(allNodes::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Get the coordinator for a region
     */
    public Node getRegionCoordinator(String regionId) {
        return regionCoordinators.get(regionId);
    }

    /**
     * Get the coordinator for a zone
     */
    public Node getZoneCoordinator(String zoneId) {
        return zoneCoordinators.get(zoneId);
    }

    /**
     * Get root coordinator(s)
     */
    public List<Node> getRootCoordinators() {
        return new ArrayList<>(rootNodes.values());
    }

    /**
     * Get all regions
     */
    public Set<String> getAllRegions() {
        return new HashSet<>(regionToZones.keySet());
    }

    /**
     * Get all zones in a region
     */
    public Set<String> getZonesInRegion(String regionId) {
        Set<String> zones = regionToZones.get(regionId);
        return zones != null ? new HashSet<>(zones) : Collections.emptySet();
    }

    /**
     * Get region for a zone
     */
    public String getRegionForZone(String zoneId) {
        return zoneToRegion.get(zoneId);
    }

    /**
     * Get zone for a node
     */
    public String getZoneForNode(String nodeId) {
        return nodeToZone.get(nodeId);
    }

    /**
     * Get total number of nodes
     */
    public int getTotalNodes() {
        return allNodes.size();
    }

    /**
     * Get number of healthy nodes
     */
    public int getHealthyNodesCount() {
        return (int) allNodes.values().stream()
                .filter(Node::isHealthy)
                .count();
    }

    /**
     * Calculate network cost between two nodes
     * Higher cost = more expensive to transfer data
     */
    public double calculateNetworkCost(String nodeId1, String nodeId2) {
        Node node1 = allNodes.get(nodeId1);
        Node node2 = allNodes.get(nodeId2);

        if (node1 == null || node2 == null) {
            return Double.MAX_VALUE;
        }

        // Same node - no cost
        if (nodeId1.equals(nodeId2)) {
            return 0.0;
        }

        // Same zone - low cost
        if (node1.getZoneId().equals(node2.getZoneId())) {
            return 1.0;
        }

        // Same region, different zones - medium cost
        if (node1.getRegionId().equals(node2.getRegionId())) {
            return 5.0;
        }

        // Different regions - high cost
        // Adjust based on tier difference
        double tierMultiplier = calculateTierMultiplier(node1.getTier(), node2.getTier());
        return 20.0 * tierMultiplier;
    }

    /**
     * Calculate network cost between two regions
     */
    public double calculateRegionNetworkCost(String regionId1, String regionId2) {
        if (regionId1.equals(regionId2)) {
            return 0.0;
        }

        // In production, this would use real network measurements
        // For now, use a simple heuristic
        return 10.0;
    }

    private double calculateTierMultiplier(DataCenterTier tier1, DataCenterTier tier2) {
        // Higher cost for cross-tier communication
        int tierDiff = Math.abs(tier1.ordinal() - tier2.ordinal());
        return 1.0 + (tierDiff * 0.5);
    }

    /**
     * Find the optimal region for data placement based on access patterns
     */
    public String findOptimalRegionForCollection(String collection, Set<String> accessingRegions) {
        if (accessingRegions.isEmpty()) {
            // Return region with most capacity
            return getRegionWithMostCapacity();
        }

        // Simple strategy: choose region that minimizes total latency to accessing regions
        String optimalRegion = null;
        double minTotalCost = Double.MAX_VALUE;

        for (String candidateRegion : getAllRegions()) {
            double totalCost = 0.0;
            for (String accessingRegion : accessingRegions) {
                totalCost += calculateRegionNetworkCost(candidateRegion, accessingRegion);
            }

            if (totalCost < minTotalCost) {
                minTotalCost = totalCost;
                optimalRegion = candidateRegion;
            }
        }

        return optimalRegion != null ? optimalRegion : getRegionWithMostCapacity();
    }

    private String getRegionWithMostCapacity() {
        return regionToZones.keySet().stream()
                .max(Comparator.comparingInt(region -> getNodesInRegion(region).size()))
                .orElse(null);
    }

    /**
     * Estimate data transfer cost for a join operation
     */
    public double estimateJoinTransferCost(String buildRegion, String probeRegion, long dataSizeBytes) {
        double costPerByte = calculateRegionNetworkCost(buildRegion, probeRegion) / 1000.0;
        return dataSizeBytes * costPerByte;
    }

    /**
     * Get topology as a hierarchical tree structure
     */
    public Map<String, Object> getTopologyTree() {
        Map<String, Object> tree = new LinkedHashMap<>();
        tree.put("rootNodes", rootNodes.keySet());

        Map<String, Object> regions = new LinkedHashMap<>();
        for (String regionId : regionToZones.keySet()) {
            Map<String, Object> regionInfo = new LinkedHashMap<>();
            regionInfo.put("coordinator", regionCoordinators.get(regionId) != null ?
                    regionCoordinators.get(regionId).getNodeId() : null);

            Map<String, Object> zones = new LinkedHashMap<>();
            for (String zoneId : regionToZones.get(regionId)) {
                Map<String, Object> zoneInfo = new LinkedHashMap<>();
                zoneInfo.put("coordinator", zoneCoordinators.get(zoneId) != null ?
                        zoneCoordinators.get(zoneId).getNodeId() : null);

                List<Map<String, String>> nodes = zoneToNodes.getOrDefault(zoneId, Collections.emptySet())
                        .stream()
                        .map(nodeId -> {
                            Node node = allNodes.get(nodeId);
                            Map<String, String> nodeInfo = new LinkedHashMap<>();
                            nodeInfo.put("nodeId", nodeId);
                            nodeInfo.put("role", node.getRole().name());
                            nodeInfo.put("healthy", String.valueOf(node.isHealthy()));
                            nodeInfo.put("collections", String.valueOf(node.getHostedCollections().size()));
                            return nodeInfo;
                        })
                        .collect(Collectors.toList());

                zoneInfo.put("nodes", nodes);
                zones.put(zoneId, zoneInfo);
            }

            regionInfo.put("zones", zones);
            regionInfo.put("totalNodes", getNodesInRegion(regionId).size());
            regionInfo.put("healthyNodes", getNodesInRegion(regionId).stream()
                    .filter(Node::isHealthy).count());

            regions.put(regionId, regionInfo);
        }

        tree.put("regions", regions);
        tree.put("totalNodes", getTotalNodes());
        tree.put("healthyNodes", getHealthyNodesCount());

        return tree;
    }

    /**
     * Validate topology consistency
     */
    public List<String> validateTopology() {
        List<String> issues = new ArrayList<>();

        // Check for nodes without zones
        for (Node node : allNodes.values()) {
            if (!zoneToNodes.containsKey(node.getZoneId())) {
                issues.add("Node " + node.getNodeId() + " has zone " + node.getZoneId() +
                        " but zone not found in topology");
            }
        }

        // Check for zones without regions
        for (String zoneId : zoneToRegion.keySet()) {
            if (!regionToZones.containsKey(zoneToRegion.get(zoneId))) {
                issues.add("Zone " + zoneId + " has region " + zoneToRegion.get(zoneId) +
                        " but region not found in topology");
            }
        }

        // Check for collections without primary region
        for (String collection : collectionToNodes.keySet()) {
            if (!collectionToPrimaryRegion.containsKey(collection)) {
                issues.add("Collection " + collection + " has no primary region assigned");
            }
        }

        return issues;
    }

    /**
     * Get statistics about the cluster
     */
    public Map<String, Object> getClusterStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();

        stats.put("totalNodes", getTotalNodes());
        stats.put("healthyNodes", getHealthyNodesCount());
        stats.put("unhealthyNodes", getTotalNodes() - getHealthyNodesCount());

        Map<String, Integer> roleDistribution = new LinkedHashMap<>();
        for (NodeRole role : NodeRole.values()) {
            roleDistribution.put(role.name(), getNodesByRole(role).size());
        }
        stats.put("roleDistribution", roleDistribution);

        Map<String, Integer> regionDistribution = new LinkedHashMap<>();
        for (String regionId : getAllRegions()) {
            regionDistribution.put(regionId, getNodesInRegion(regionId).size());
        }
        stats.put("regionDistribution", regionDistribution);

        // Collection distribution
        Map<String, Integer> collectionDistribution = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : collectionToNodes.entrySet()) {
            collectionDistribution.put(entry.getKey(), entry.getValue().size());
        }
        stats.put("collectionDistribution", collectionDistribution);

        // Average collections per node
        double avgCollections = allNodes.values().stream()
                .mapToInt(node -> node.getHostedCollections().size())
                .average()
                .orElse(0.0);
        stats.put("avgCollectionsPerNode", avgCollections);

        return stats;
    }

    /**
     * Update network latency between two nodes
     */
    public void updateLatency(String nodeId1, String nodeId2, int latencyMs) {
        latencyMatrix.computeIfAbsent(nodeId1, k -> new ConcurrentHashMap<>())
                .put(nodeId2, latencyMs);
        latencyMatrix.computeIfAbsent(nodeId2, k -> new ConcurrentHashMap<>())
                .put(nodeId1, latencyMs);
    }

    /**
     * Get estimated latency between two nodes
     */
    public int getEstimatedLatency(String nodeId1, String nodeId2) {
        if (nodeId1.equals(nodeId2)) {
            return 0;
        }

        Map<String, Integer> node1Latencies = latencyMatrix.get(nodeId1);
        if (node1Latencies != null && node1Latencies.containsKey(nodeId2)) {
            return node1Latencies.get(nodeId2);
        }

        // Fallback to topology-based estimation
        Node node1 = allNodes.get(nodeId1);
        Node node2 = allNodes.get(nodeId2);

        if (node1 == null || node2 == null) {
            return 100; // Default high latency
        }

        if (node1.getZoneId().equals(node2.getZoneId())) {
            return 1; // Same zone
        }

        if (node1.getRegionId().equals(node2.getRegionId())) {
            return 5; // Same region, different zone
        }

        return 20; // Different regions
    }

    /**
     * Find the closest coordinator for a node
     */
    public Node findClosestCoordinator(String nodeId, NodeRole coordinatorRole) {
        Node node = allNodes.get(nodeId);
        if (node == null) {
            return null;
        }

        List<Node> coordinators = getNodesByRole(coordinatorRole);
        if (coordinators.isEmpty()) {
            return null;
        }

        // Find coordinator with minimum network cost
        Node closest = null;
        double minCost = Double.MAX_VALUE;

        for (Node coordinator : coordinators) {
            double cost = calculateNetworkCost(nodeId, coordinator.getNodeId());
            if (cost < minCost) {
                minCost = cost;
                closest = coordinator;
            }
        }

        return closest;
    }
}