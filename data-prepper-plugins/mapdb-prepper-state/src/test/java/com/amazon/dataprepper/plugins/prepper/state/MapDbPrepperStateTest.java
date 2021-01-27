package com.amazon.dataprepper.plugins.prepper.state;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MapDbPrepperStateTest extends PrepperStateTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    public void setPrepperState() throws Exception {
        this.prepperState = new MapDbPrepperState<>(temporaryFolder.newFolder(), "testDb", 16);
    }

    @Test
    public void testIterateSegment() throws IOException {
        final byte[] key1  = new byte[]{-64, 0, -64, 0};
        final byte[] key2 = new byte[]{0};
        final byte[] key3 = new byte[]{64, 64, 64 , 64};
        final byte[] key4 = new byte[]{126, 126, 126, 126};

        final DataClass data1 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final DataClass data2 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final DataClass data3 = new DataClass(UUID.randomUUID().toString(), random.nextInt());
        final DataClass data4 = new DataClass(UUID.randomUUID().toString(), random.nextInt());

        prepperState.put(key3, data3);
        prepperState.put(key4, data4);
        prepperState.put(key1, data1);
        prepperState.put(key2, data2);

        final List<String> values = prepperState.iterate(new BiFunction<byte[], DataClass, String>() {
            @Override
            public String apply(byte[] bytes, DataClass s) {
                return s.stringVal;
            }
        }, 2, 0);

        final List<String> values2 = prepperState.iterate(new BiFunction<byte[], DataClass, String>() {
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

        Assert.assertEquals( 1048576, prepperState.sizeInBytes());
    }

}
