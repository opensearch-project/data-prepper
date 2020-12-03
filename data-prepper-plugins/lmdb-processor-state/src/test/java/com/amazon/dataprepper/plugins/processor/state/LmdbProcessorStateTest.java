package com.amazon.dataprepper.plugins.processor.state;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LmdbProcessorStateTest extends ProcessorStateTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    public void setProcessorState() throws Exception {
        this.processorState = new LmdbProcessorState<>(temporaryFolder.newFile(), "testDb", DataClass.class);
    }

    @Test
    public void testPutAll() {
        final byte[] key1 = new byte[8];
        random.nextBytes(key1);
        final byte[] key2 = new byte[8];
        random.nextBytes(key2);

        DataClass value1 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        DataClass value2 = new DataClass(UUID.randomUUID().toString(), random.nextInt());

        final Map<byte[], DataClass> batch = new HashMap<byte[], DataClass>(){{
            put(key1, value1);
            put(key2, value2);
        }};

        ((LmdbProcessorState)processorState).putAll(batch);

        Assert.assertEquals(value1, processorState.get(key1));
        Assert.assertEquals(value2, processorState.get(key2));

    }
}
