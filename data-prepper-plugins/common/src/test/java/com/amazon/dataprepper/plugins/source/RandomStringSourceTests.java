/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.TestBuffer;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class RandomStringSourceTests {

    @Test
    public void testPutRecord() throws ExecutionException, InterruptedException {
        final RandomStringSource randomStringSource = new RandomStringSource();
        final Queue<Record<Event>> bufferQueue = new LinkedBlockingQueue<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> task = executorService.submit(() -> randomStringSource.start(buffer));

        try {
            // this timeout is increased to 3000 to ensure that at least 1 Record has time to be made by the RandomStringSource
            task.get(3000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            assertThat(buffer.size(), greaterThan(0));
            randomStringSource.stop();
            assertThat(buffer.size(), greaterThan(0));
        }
    }

}
