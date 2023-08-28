package org.opensearch.dataprepper.plugins.sink.s3;

/**
 * Represents common size combinations.
 */
final class SizeCombination {
    static final SizeCombination EXACTLY_ONE = new SizeCombination(1, 1);
    static final SizeCombination MEDIUM_SMALLER = new SizeCombination(100, 10);
    static final SizeCombination MEDIUM_LARGER = new SizeCombination(500, 50);
    static final SizeCombination LARGE = new SizeCombination(500, 500);

    private final int batchSize;

    private final int numberOfBatches;

    private SizeCombination(int batchSize, int numberOfBatches) {
        this.batchSize = batchSize;
        this.numberOfBatches = numberOfBatches;
    }

    int getTotalSize() {
        return batchSize * numberOfBatches;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getNumberOfBatches() {
        return numberOfBatches;
    }

    @Override
    public String toString() {
        return "batchSize=" + batchSize +",batches=" + numberOfBatches;
    }
}
