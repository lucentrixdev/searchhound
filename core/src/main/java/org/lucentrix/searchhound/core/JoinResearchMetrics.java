package org.lucentrix.searchhound.core;

import java.util.HashMap;
import java.util.Map;

public class JoinResearchMetrics {
    private final Map<String, Metric> metrics = new HashMap<>();

    public void record(String metricName, double value) {
        metrics.computeIfAbsent(metricName, k -> new Metric()).add(value);
    }

    public void printReport() {
        System.out.println("=== DISTRIBUTED JOIN RESEARCH METRICS ===");
        metrics.forEach((name, metric) -> {
            System.out.printf("%s: avg=%.2f, min=%.2f, max=%.2f, count=%d%n",
                    name, metric.average(), metric.min(), metric.max(), metric.count());
        });
    }

    static class Metric {
        private double sum = 0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private int count = 0;

        public void add(double value) {
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }

        public double average() { return count > 0 ? sum / count : 0; }
        public double min() { return min; }
        public double max() { return max; }
        public int count() { return count; }
    }
}
