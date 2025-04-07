package org.opensearch.dataprepper.plugins.source.oteltelemetry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OTelTelemetrySourceTest {
    @Mock
    private Buffer<Record<Object>> buffer;

    private OTelTelemetrySource source;

    @BeforeEach
    void setUp() {
        OTelTelemetrySourceConfig config = new OTelTelemetrySourceConfig();
        buffer = mock(Buffer.class);
        source = new OTelTelemetrySource(config);
    }

    @Test
    void testStart() {
        source.start(buffer);
        // Verify that the source starts without exceptions
    }

    @Test
    void testStop() {
        source.stop();
        // Verify that the source stops without exceptions
    }
}
