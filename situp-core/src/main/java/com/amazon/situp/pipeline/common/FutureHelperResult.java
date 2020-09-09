package com.amazon.situp.pipeline.common;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FutureHelperResult<T> {
    private final List<T> successfulResults;
    private final List<Future<T>> uncompletedFutures;
    private final List<Integer> uncompletedIndexes;
    private final List<ExecutionException> failedReasons;
    private final List<Integer> failedIndexes;

    public FutureHelperResult(
            List<T> successfulResults,
            List<Future<T>> uncompletedFutures,
            List<Integer> uncompletedIndexes,
            List<ExecutionException> failedReasons,
            List<Integer> failedIndexes) {
        this.successfulResults = successfulResults;
        this.uncompletedFutures = uncompletedFutures;
        this.uncompletedIndexes = uncompletedIndexes;
        this.failedReasons = failedReasons;
        this.failedIndexes = failedIndexes;
    }

    public FutureHelperResult(List<T> successfulResults, List<ExecutionException> failedReasons) {
        this(successfulResults, Collections.emptyList(), Collections.emptyList(), failedReasons,
                Collections.emptyList());
    }

    public List<T> getSuccessfulResults() {
        return successfulResults;
    }

    public List<Future<T>> getUncompletedFutures() {
        return uncompletedFutures;
    }

    public List<Integer> getUncompletedIndexes() {
        return uncompletedIndexes;
    }

    public List<ExecutionException> getFailedReasons() {
        return failedReasons;
    }

    public List<Integer> getFailedIndexes() {
        return failedIndexes;
    }
}
