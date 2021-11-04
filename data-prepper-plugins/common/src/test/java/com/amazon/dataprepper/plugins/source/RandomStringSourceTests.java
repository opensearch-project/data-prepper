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

import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.TestBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;

public class RandomStringSourceTests {

    @Test
    public void testPutRecord() throws InterruptedException {
        final RandomStringSource randomStringSource =
                new RandomStringSource();
        final Queue<Record<String>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);
        //Start source, and sleep for 100 millis
        randomStringSource.start(buffer);
        Thread.sleep(100);
        //Make sure that 1 record is in buffer
        Assert.assertEquals(1, buffer.size());
        //Stop the source, and wait long enough that another message would be sent
        //if the source was running
        randomStringSource.stop();
        Thread.sleep(500);
        //Make sure there is still only 1 record in buffer
        Assert.assertEquals(1, buffer.size());
    }

}
