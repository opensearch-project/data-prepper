/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import com.amazon.dataprepper.plugins.source.codec.NewlineDelimitedCodec;
import com.amazon.dataprepper.plugins.source.codec.NewlineDelimitedConfig;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.amazon.dataprepper.plugins.source.S3ObjectWorker.S3_OBJECTS_FAILED_METRIC_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3ObjectWorkerIT {
    public static final int NUMBER_OF_RECORDS_TO_ACCUMULATE = 100;
    public static final int TIMEOUT_IN_MILLIS = 200;
    private S3Client s3Client;
    private S3ObjectGenerator s3ObjectGenerator;
    private Buffer<Record<Event>> buffer;
    private String bucket;
    private int recordsReceived;
    private PluginMetrics pluginMetrics;

    @BeforeEach
    void setUp() throws Exception {
        s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        bucket = System.getProperty("tests.s3source.bucket");
        s3ObjectGenerator = new S3ObjectGenerator(s3Client, bucket);

        buffer = mock(Buffer.class);
        recordsReceived = 0;
        doAnswer(a -> {
            final Collection<Record<Event>> recordsCollection = a.<Collection<Record<Event>>>getArgument(0);
            assertThat(recordsCollection.size(), greaterThanOrEqualTo(1));
            for (Record<Event> eventRecord : recordsCollection) {
                assertThat(eventRecord, notNullValue());
                assertThat(eventRecord.getData(), notNullValue());
                assertThat(eventRecord.getData().get("message", String.class), notNullValue());

            }
            recordsReceived += recordsCollection.size();
            return null;
        })
                .when(buffer).writeAll(anyCollection(), anyInt());

        pluginMetrics = mock(PluginMetrics.class);
        final Counter counter = mock(Counter.class);
        when(pluginMetrics.counter(S3_OBJECTS_FAILED_METRIC_NAME)).thenReturn(counter);
    }

    private S3ObjectWorker createObjectUnderTest(final Codec codec, final int numberOfRecordsToAccumulate) {
        return new S3ObjectWorker(s3Client, buffer, codec, Duration.ofMillis(TIMEOUT_IN_MILLIS), numberOfRecordsToAccumulate, pluginMetrics);
    }

    @ParameterizedTest
    @ArgumentsSource(RecordAndAccumulationCounts.class)
    void get_newline_delimited_object(final int numberOfRecords, final int numberOfRecordsToAccumulate) throws Exception {

        final String key = "s3source/newlines/" + numberOfRecords + "_" + Instant.now().toString() + ".json";
        s3ObjectGenerator.write(numberOfRecords, key, new NewlineDelimitedRecordsGenerator());

        final S3ObjectWorker objectUnderTest = createObjectUnderTest(new NewlineDelimitedCodec(new NewlineDelimitedConfig()), numberOfRecordsToAccumulate);

        final S3ObjectReference s3ObjectReference = S3ObjectReference.fromBucketAndKey(bucket, key);
        objectUnderTest.parseS3Object(s3ObjectReference);

        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);

        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));

        assertThat(recordsReceived, equalTo(numberOfRecords));
    }

    static class RecordAndAccumulationCounts implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            final List<Integer> numberOfRecordsList = List.of(0, 1, 25, 500, 5000);
            final List<Integer> recordsToAccumulateList = List.of(1, 100, 1000);

            return numberOfRecordsList
                    .stream()
                    .flatMap(records -> recordsToAccumulateList
                            .stream()
                            .map(accumulate -> arguments(records, accumulate))
                    );
        }
    }
}
