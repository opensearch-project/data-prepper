package com.amazon.situp.plugins.processor;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class KeyRangeSplitterTest {

    @Test
    public void testKeyRanges() {
        final KeyRangeSplitter keyRangeSplitter = new KeyRangeSplitter("A", "G", StandardCharsets.UTF_8, 1);
        final String bound1 = keyRangeSplitter.getBoundary(0, 2);
        final String bound2 = keyRangeSplitter.getBoundary(1, 2);
        final String bound3 = keyRangeSplitter.getBoundary(2, 2);

        Assert.assertEquals("A", bound1);
        Assert.assertEquals("D", bound2);
        Assert.assertEquals("G", bound3);
    }

}
