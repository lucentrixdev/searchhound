package org.lucentrix.searchhound.core;

import org.lucentrix.searchhound.topology.ClusterTopology;

import java.util.Map;

public class JoinQueryPlanner {

    public enum JoinStrategy {
        BROADCAST_HASH_JOIN,
        DISTRIBUTED_HASH_JOIN,
        SORT_MERGE_JOIN,
        NESTED_LOOP_JOIN
    }

    public static class JoinPlan {
        private final String buildSide;
        private final String probeSide;
        private final JoinStrategy strategy;
        private final double estimatedCost;

        public JoinPlan(String buildSide, String probeSide, JoinStrategy strategy, double cost) {
            this.buildSide = buildSide;
            this.probeSide = probeSide;
            this.strategy = strategy;
            this.estimatedCost = cost;
        }
    }

    /**
     * Generate optimal join plan based on statistics
     */
    public JoinPlan planJoin(
            String leftTable, String rightTable,
            Map<String, CollectionStats> stats,
            ClusterTopology topology) {

        // Calculate costs for different strategies
        double broadcastCost = calculateBroadcastHashCost(leftTable, rightTable, stats, topology);
        double distributedCost = calculateDistributedHashCost(leftTable, rightTable, stats, topology);

        // Choose best strategy
        if (broadcastCost < distributedCost) {
            // Choose smaller table as build side
            String buildSide = stats.get(leftTable).cardinality() < stats.get(rightTable).cardinality()
                    ? leftTable : rightTable;
            String probeSide = buildSide.equals(leftTable) ? rightTable : leftTable;
            return new JoinPlan(buildSide, probeSide, JoinStrategy.BROADCAST_HASH_JOIN, broadcastCost);
        } else {
            // Use distributed hash join
            return new JoinPlan(leftTable, rightTable, JoinStrategy.DISTRIBUTED_HASH_JOIN, distributedCost);
        }
    }

    private double calculateBroadcastHashCost(String left, String right,
                                              Map<String, CollectionStats> stats,
                                              ClusterTopology topology) {
        // Calculate network transfer + local join cost for broadcast
        // Simplified calculation
        long smallerSize = Math.min(stats.get(left).sizeInBytes(), stats.get(right).sizeInBytes());
        long largerSize = Math.max(stats.get(left).sizeInBytes(), stats.get(right).sizeInBytes());

        // Broadcast cost = smallerSize * numNodes + localJoinCost
        return smallerSize * topology.getTotalNodes() + (largerSize * 0.1); // 0.1 is local join factor
    }

    private double calculateDistributedHashCost(String left, String right,
                                                Map<String, CollectionStats> stats,
                                                ClusterTopology topology) {
        // Calculate network transfer for distributed hash join
        long leftSize = stats.get(left).sizeInBytes();
        long rightSize = stats.get(right).sizeInBytes();

        // Shuffle both sides + local join
        return (leftSize + rightSize) * 0.5 + // Network transfer (assuming 50% reduction with Bloom filters)
                Math.max(leftSize, rightSize) * 0.05; // Local join
    }
}

class CollectionStats {
    private final long cardinality;
    private final long sizeInBytes;
    private final double selectivity;

    public CollectionStats(long cardinality, long sizeInBytes, double selectivity) {
        this.cardinality = cardinality;
        this.sizeInBytes = sizeInBytes;
        this.selectivity = selectivity;
    }

    // Getters
    public long cardinality() {
        return cardinality;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public double selectivity() {
        return selectivity;
    }
}
