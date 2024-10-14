/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelperResult;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FutureHelperTest {

    @Mock
    Future successfulFuture;
    @Mock
    Future failedFuture;
    @Mock
    Future interuptedFuture;

    @Before
    public void setup() throws Exception {
        when(failedFuture.get()).thenThrow(ExecutionException.class);
        when(interuptedFuture.get()).thenThrow(InterruptedException.class);
    }

    @Test
    public void awaitFuturesIndefinitelyWithSuccessfulFuture() {
        FutureHelperResult result = FutureHelper.awaitFuturesIndefinitely(Arrays.asList(successfulFuture));

        assertEquals(1, result.getSuccessfulResults().size());
        assertEquals(0, result.getFailedReasons().size());
    }

    @Test
    public void awaitFuturesIndefinitelyWithFailedFuture() {
        FutureHelperResult result = FutureHelper.awaitFuturesIndefinitely(Arrays.asList(failedFuture));

        assertEquals(0, result.getSuccessfulResults().size());
        assertEquals(1, result.getFailedReasons().size());
    }

    @Test
    public void awaitFuturesIndefinitelyWithInterruptedFuture() {
        FutureHelperResult result = FutureHelper.awaitFuturesIndefinitely(Arrays.asList(interuptedFuture));

        assertEquals(0, result.getSuccessfulResults().size());
        assertEquals(0, result.getFailedReasons().size());
    }
}
