/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.state;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

public class MapDbProcessorStateTest extends ProcessorStateTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    public void setProcessorState() throws Exception {
        this.processorState = new MapDbProcessorState<>(temporaryFolder.newFolder(), "testDb", 16);
    }

    @Test
    public void testIterateSegment() throws IOException {
        final byte[] key1 = new byte[]{-64, 0, -64, 0};
        final byte[] key2 = new byte[]{0};
        final byte[] key3 = new byte[]{64, 64, 64, 64};
        final byte[] key4 = new byte[]{126, 126, 126, 126};

        final DataClass data1 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final DataClass data2 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final DataClass data3 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final DataClass data4 = new DataClass(UUID.randomUUID().toString(), random.nextInt());

        processorState.put(key3, data3);
        processorState.put(key4, data4);
        processorState.put(key1, data1);
        processorState.put(key2, data2);

        final List<String> values = processorState.iterate(new BiFunction<byte[], DataClass, String>() {
            @Override
            public String apply(byte[] bytes, DataClass s) {
                return s.stringVal;
            }
        }, 2, 0);

        final List<String> values2 = processorState.iterate(new BiFunction<byte[], DataClass, String>() {
            @Override
            public String apply(byte[] bytes, DataClass s) {
                return s.stringVal;
            }
        }, 2, 1);

        Assert.assertEquals(2, values.size());
        Assert.assertEquals(2, values2.size());
        Assert.assertTrue(values.containsAll(Arrays.asList(
                data1.stringVal,
                data2.stringVal
        )));
        Assert.assertTrue(values2.containsAll(Arrays.asList(
                data3.stringVal,
                data4.stringVal
        )));
    }

}
