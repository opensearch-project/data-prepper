package com.amazon.situp.processor.state;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class ProcessorStateTests {

    protected ProcessorState<DataClass> processorState;
    private static final Random random = new Random();

    @Before
    public abstract void setProcessorState() throws Exception;

    @Test
    public void testPutAndGet() {
        //Put two random Pojos in the processor state
        final DataClass data1 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final String key1 = UUID.randomUUID().toString();

        final DataClass data2 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final String key2 = UUID.randomUUID().toString();

        processorState.put(key1, data1);
        processorState.put(key2, data2);

        //Read them and assert that they are correctly read, and assert incorrect key gives back null value
        Assert.assertEquals(data1, processorState.get(key1));
        Assert.assertEquals(data2, processorState.get(key2));
        Assert.assertNull(processorState.get(UUID.randomUUID().toString()));
    }

    @Test
    public void testPutAndGetAll() {
        //Put two random Pojos in the processor state
        final DataClass data1 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final String key1 = UUID.randomUUID().toString();

        final DataClass data2 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final String key2 = UUID.randomUUID().toString();

        processorState.put(key1, data1);
        processorState.put(key2, data2);

        final Map<String, DataClass> stateMap = processorState.getAll();
        Assert.assertEquals(2, stateMap.size());
        Assert.assertEquals(data1, stateMap.get(key1));
        Assert.assertEquals(data2, stateMap.get(key2));
    }

    @After
    public void teardown() {
        processorState.clear();
        processorState.close();
    }


    public static class DataClass {
        public String stringVal;
        public int intVal;

        public DataClass(){}

        public DataClass(final String stringVal, final int intVal) {
            this.stringVal = stringVal;
            this.intVal = intVal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataClass dataClass = (DataClass) o;
            return intVal == dataClass.intVal &&
                    Objects.equals(stringVal, dataClass.stringVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stringVal, intVal);
        }
        
    }

}
