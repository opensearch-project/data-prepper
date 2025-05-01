package org.opensearch.dataprepper.core.parser.config;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DisableMetricsFilterTest {

    @ParameterizedTest
    @CsvSource({
            "jvm.gc.max.data.size, jvm.gc.max.data.size, DENY",
            "jvm.gc.max.data.size, jvm.gc.**, DENY",
            "test-pipeline.http.successRequests, **.http.successRequests, DENY",
            "test-pipeline.http.successRequests, jvm.gc.**, NEUTRAL",
            "metric.allowed, jvm.gc.max.data.size, NEUTRAL"
    })
    void testDisabledMetricsFilter(String metricName, String pattern, MeterFilterReply expectedReply) {
        final DisableMetricsFilter filter = new DisableMetricsFilter(List.of(pattern));

        final Id id = new Id(metricName, Tags.empty(), null, null, Type.COUNTER);
        final MeterFilterReply result = filter.accept(id);

        assertEquals(expectedReply, result);
    }
}
