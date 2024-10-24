/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.common;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class FutureHelperResultTest {

    private List<Void> successfulResults;
    private List<Future<Void>> uncompletedFutures;
    private List<Integer> uncompletedIndexes;
    private List<ExecutionException> failedReasons;
    private List<Integer> failedIndexes;

    private FutureHelperResult<Void> sut;

    @Before
    public void setup() {
        successfulResults = new ArrayList<>();
        uncompletedFutures = Collections.singletonList(CompletableFuture.completedFuture(null));
        uncompletedIndexes = Collections.singletonList(123);
        failedReasons = Collections.singletonList(new ExecutionException(new Throwable()));
        failedIndexes = Collections.singletonList(456);
    }

    @Test
    public void testGetters() {
        sut = new FutureHelperResult<>(successfulResults,
                uncompletedFutures,
                uncompletedIndexes,
                failedReasons,
                failedIndexes);

        assertEquals(successfulResults, sut.getSuccessfulResults());
        assertEquals(uncompletedFutures, sut.getUncompletedFutures());
        assertEquals(uncompletedIndexes, sut.getUncompletedIndexes());
        assertEquals(failedReasons, sut.getFailedReasons());
        assertEquals(failedIndexes, sut.getFailedIndexes());
    }

    @Test
    public void testShortConstructor() {
        sut = new FutureHelperResult<>(successfulResults,
                failedReasons);

        assertEquals(successfulResults, sut.getSuccessfulResults());
        assertEquals(Collections.emptyList(), sut.getUncompletedFutures());
        assertEquals(Collections.emptyList(), sut.getUncompletedIndexes());
        assertEquals(failedReasons, sut.getFailedReasons());
        assertEquals(Collections.emptyList(), sut.getFailedIndexes());
    }

}
