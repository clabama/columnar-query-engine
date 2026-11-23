package dev.wixforth.columnar.exec;

/**
 * Execution telemetry recorded for every query run.
 */
public record ExecutionStats(
        long elapsedNanos,
        int partitionsTotal,
        int partitionsPruned,
        int partitionsScanned,
        long bytesRead,
        long totalTableBytes,
        long cacheHits,
        long cacheMisses,
        int threadsUsed) {

    public double elapsedMillis() {
        return elapsedNanos / 1_000_000.0;
    }

    public double partitionPrunedPercent() {
        return partitionsTotal == 0 ? 0.0 : (100.0 * partitionsPruned / partitionsTotal);
    }

    public double bytesSkippedPercent() {
        return totalTableBytes == 0 ? 0.0 : (100.0 * (totalTableBytes - bytesRead) / totalTableBytes);
    }
}
