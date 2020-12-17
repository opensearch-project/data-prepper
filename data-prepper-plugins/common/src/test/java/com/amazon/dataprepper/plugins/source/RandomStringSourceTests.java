package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.BlockingBuffer;
import com.amazon.dataprepper.plugins.buffer.TestBuffer;
import java.util.LinkedList;
import java.util.Queue;
import org.junit.Assert;
import org.junit.Test;

public class RandomStringSourceTests {

    @Test
    public void testStartAndStop() throws InterruptedException {
        final RandomStringSource randomStringSource = new RandomStringSource(null);
        final Queue<Record<String>> bufferQueue = new LinkedList<>();
        final TestBuffer buffer = new TestBuffer(bufferQueue, 1);

        randomStringSource.start(buffer);
        Thread.sleep(300);
        Assert.assertEquals(1, buffer.size());
        randomStringSource.stop();
        Thread.sleep(500);
        Assert.assertEquals(1, buffer.size());
    }
}
