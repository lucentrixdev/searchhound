package org.lucentrix.searchhound.topology;

import java.util.*;

/**
 * Builder for creating complex cluster topologies
 */
public class ClusterTopologyBuilder {

    private final ClusterTopology topology;
    private final Random random = new Random();

    public ClusterTopologyBuilder(String rootNodeId) {
        this.topology = new ClusterTopology(rootNodeId);
    }

    /**
     * Build a realistic test topology
     */
    public ClusterTopologyBuilder buildTestTopology() {
        // Create root coordinator
        ClusterTopology.Node root = new ClusterTopology.Node(
                "root-1", "10.0.0.1", 9200,
                ClusterTopology.NodeRole.ROOT_COORDINATOR,
                ClusterTopology.DataCenterTier.TIER_1,
                "global-zone", "global-region"
        );
        topology.addNode(root);

        // Create 3 regions
        String[] regions = {"us-east", "us-west", "eu-central"};
        ClusterTopology.DataCenterTier[] regionTiers = {
                ClusterTopology.DataCenterTier.TIER_1,
                ClusterTopology.DataCenterTier.TIER_2,
                ClusterTopology.DataCenterTier.TIER_1
        };

        for (int r = 0; r < regions.length; r++) {
            String region = regions[r];

            // Region coordinator
            ClusterTopology.Node regionCoord = new ClusterTopology.Node(
                    "region-" + region + "-coord",
                    "10." + (r+1) + ".0.1", 9200,
                    ClusterTopology.NodeRole.REGION_COORDINATOR,
                    regionTiers[r],
                    region + "-zone-0", region
            );
            topology.addNode(regionCoord);

            // Create 2 zones per region
            for (int z = 0; z < 2; z++) {
                String zone = region + "-zone-" + z;

                // Zone coordinator
                ClusterTopology.Node zoneCoord = new ClusterTopology.Node(
                        "zone-" + zone + "-coord",
                        "10." + (r+1) + "." + (z+1) + ".1", 9200,
                        ClusterTopology.NodeRole.ZONE_COORDINATOR,
                        regionTiers[r],
                        zone, region
                );
                topology.addNode(zoneCoord);

                // Create 3 SearchLeaf nodes per zone
                for (int n = 0; n < 3; n++) {
                    ClusterTopology.Node leaf = new ClusterTopology.Node(
                            "leaf-" + zone + "-" + n,
                            "10." + (r+1) + "." + (z+1) + "." + (n+10), 9200,
                            ClusterTopology.NodeRole.SEARCH_LEAF,
                            regionTiers[r],
                            zone, region
                    );

                    // Add some sample collections
                    leaf.addCollection("users", 10000 + random.nextInt(50000));
                    leaf.addCollection("orders", 50000 + random.nextInt(100000));
                    if (random.nextBoolean()) {
                        leaf.addCollection("products", 5000 + random.nextInt(20000));
                    }

                    topology.addNode(leaf);
                }
            }
        }

        return this;
    }

    /**
     * Add Bloom filter service nodes
     */
    public ClusterTopologyBuilder withBloomFilterServices() {
        String[] regions = {"us-east", "us-west", "eu-central"};

        for (String region : regions) {
            ClusterTopology.Node bfService = new ClusterTopology.Node(
                    "bf-service-" + region,
                    "10.0.0.100", 9300, // Different port
                    ClusterTopology.NodeRole.BLOOM_FILTER_SERVICE,
                    ClusterTopology.DataCenterTier.TIER_1,
                    region + "-bf-zone", region
            );
            bfService.putMetadata("bloomFilterCapacity", "1GB");
            bfService.putMetadata("compressionEnabled", true);
            topology.addNode(bfService);
        }

        return this;
    }

    /**
     * Add join coordinator services
     */
    public ClusterTopologyBuilder withJoinCoordinators() {
        String[] regions = {"us-east", "us-west", "eu-central"};

        for (String region : regions) {
            ClusterTopology.Node joinCoord = new ClusterTopology.Node(
                    "join-coord-" + region,
                    "10.0.0.200", 9400,
                    ClusterTopology.NodeRole.JOIN_COORDINATOR,
                    ClusterTopology.DataCenterTier.TIER_1,
                    region + "-join-zone", region
            );
            joinCoord.putMetadata("maxConcurrentJoins", 100);
            joinCoord.putMetadata("adaptivePlanning", true);
            topology.addNode(joinCoord);
        }

        return this;
    }

    public ClusterTopology build() {
        return topology;
    }
}
