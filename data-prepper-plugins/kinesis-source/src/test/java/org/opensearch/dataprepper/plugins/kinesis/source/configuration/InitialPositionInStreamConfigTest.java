package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.common.InitialPositionInStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InitialPositionInStreamConfigTest {

    @Test
    void testInitialPositionGetByNameLATEST() {
        final InitialPositionInStreamConfig initialPositionInStreamConfig = InitialPositionInStreamConfig.fromPositionValue("latest");
        assertEquals(initialPositionInStreamConfig, InitialPositionInStreamConfig.LATEST);
        assertEquals(initialPositionInStreamConfig.toString(), "latest");
        assertEquals(initialPositionInStreamConfig.getPosition(), "latest");
        assertEquals(initialPositionInStreamConfig.getPositionInStream(), InitialPositionInStream.LATEST);
    }

    @Test
    void testInitialPositionGetByNameEarliest() {
        final InitialPositionInStreamConfig initialPositionInStreamConfig = InitialPositionInStreamConfig.fromPositionValue("earliest");
        assertEquals(initialPositionInStreamConfig, InitialPositionInStreamConfig.EARLIEST);
        assertEquals(initialPositionInStreamConfig.toString(), "earliest");
        assertEquals(initialPositionInStreamConfig.getPosition(), "earliest");
        assertEquals(initialPositionInStreamConfig.getPositionInStream(), InitialPositionInStream.TRIM_HORIZON);
    }

}
