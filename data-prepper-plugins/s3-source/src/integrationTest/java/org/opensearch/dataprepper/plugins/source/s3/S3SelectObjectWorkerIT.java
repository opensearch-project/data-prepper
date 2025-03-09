/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

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

class S3SelectObjectWorkerIT {
    private static final int TIMEOUT_IN_MILLIS = 200;
    private S3AsyncClient s3AsyncClient;
    private S3ObjectGenerator s3ObjectGenerator;
    private Buffer<Record<Event>> buffer;
    private String bucket;
    private int recordsReceived;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private BucketOwnerProvider bucketOwnerProvider;
    private EventMetadataModifier eventMetadataModifier;

    @BeforeEach
    void setUp() {
        S3Client s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        bucket = System.getProperty("tests.s3source.bucket");
        s3ObjectGenerator = new S3ObjectGenerator(s3Client, bucket);

        buffer = mock(Buffer.class);
        eventMetadataModifier = mock(EventMetadataModifier.class);
        recordsReceived = 0;

        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);
        final Counter counter = mock(Counter.class);
        final DistributionSummary distributionSummary = mock(DistributionSummary.class);
        final Timer timer = new NoopTimer(new Meter.Id("test", Tags.empty(), null, null, Meter.Type.TIMER));
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(timer);
        bucketOwnerProvider = b -> Optional.empty();
    }

    private void stubBufferWriter(final Consumer<Event> additionalEventAssertions, final String key) throws Exception {
        doAnswer(a -> {
            final Collection<Record<Event>> recordsCollection = a.getArgument(0);
            assertThat(recordsCollection.size(), greaterThanOrEqualTo(1));
            for (Record<Event> eventRecord : recordsCollection) {
                assertThat(eventRecord, notNullValue());
                assertThat(eventRecord.getData(), notNullValue());
                assertThat(eventRecord.getData().get("s3/bucket", String.class), equalTo(bucket));
                assertThat(eventRecord.getData().get("s3/key", String.class), equalTo(key));
                additionalEventAssertions.accept(eventRecord.getData());
            }
            recordsReceived += recordsCollection.size();
            return null;
        })
                .when(buffer).writeAll(anyCollection(), anyInt());
    }

    private S3SelectObjectWorker createObjectUnderTest(final int numberOfRecordsToAccumulate,
                                                       final CompressionType compressionType,
                                                       final RecordsGenerator recordsGenerator,
                                                       final String expression) {
        S3SelectCSVOption s3SelectCSVOption = mock(S3SelectCSVOption.class);
        when(s3SelectCSVOption.getFileHeaderInfo()).thenReturn("none");
        final S3ObjectRequest request = new S3ObjectRequest.Builder(buffer, numberOfRecordsToAccumulate,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3SelectCSVOption(s3SelectCSVOption)
                .s3SelectJsonOption(new S3SelectJsonOption())
                .s3AsyncClient(s3AsyncClient)
                .eventConsumer(eventMetadataModifier).expressionType("SQL")
                .serializationFormatOption(S3SelectSerializationFormatOption.valueOf(
                        recordsGenerator.getFileExtension().toUpperCase()))
                .expression(expression)
                .compressionType(compressionType).
                s3SelectResponseHandlerFactory(new S3SelectResponseHandlerFactory()).build();
        return new S3SelectObjectWorker(request);
    }

    @ParameterizedTest
    @ArgumentsSource(IntegrationTestArguments.class)
    void parseS3Object_correctly_loads_data_into_Buffer(
            final RecordsGenerator recordsGenerator,
            final int numberOfRecords,
            final int numberOfRecordsToAccumulate,
            final boolean shouldCompress) throws Exception {

        final String key = getKeyString(recordsGenerator, numberOfRecords, shouldCompress);
        final CompressionType compressionType = shouldCompress ? CompressionType.GZIP : CompressionType.NONE;
        final S3SelectObjectWorker objectUnderTest = createObjectUnderTest(numberOfRecordsToAccumulate,
                compressionType,
                recordsGenerator,
                recordsGenerator.getS3SelectExpression());

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, shouldCompress);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);

        parseObject(key, objectUnderTest);

        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);

        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));

        assertThat(recordsReceived, equalTo(numberOfRecords));
    }


    @ParameterizedTest
    @CsvSource({"25,10,select * from s3Object limit 25",
            "100,25,select * from s3Object limit 100",
            "50000,25,select * from s3Object limit 50000"})
    void parseS3Object_parquet_correctly_loads_data_into_Buffer(
            final int numberOfRecords,
            final int numberOfRecordsToAccumulate,
            final String expression) throws Exception {
        final RecordsGenerator parquetRecordsGenerator = new ParquetRecordsGenerator();
        final String key = getKeyString(parquetRecordsGenerator, numberOfRecords, Boolean.FALSE);
        final S3SelectObjectWorker objectUnderTest = createObjectUnderTest(numberOfRecordsToAccumulate,
                CompressionType.NONE,
                parquetRecordsGenerator,
                expression);

        s3ObjectGenerator.write(numberOfRecords, key, parquetRecordsGenerator, Boolean.FALSE);
        stubBufferWriter(parquetRecordsGenerator::assertEventIsCorrect, key);

        parseObject(key, objectUnderTest);
        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);

        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));

        assertThat(recordsReceived, equalTo(numberOfRecords));
    }

    private String getKeyString(final RecordsGenerator recordsGenerator, final int numberOfRecords, final boolean shouldCompress) {
        String key = "s3source/s3/" + numberOfRecords + "_" + Instant.now().toEpochMilli() + "." + recordsGenerator.getFileExtension();
        return shouldCompress ? key + ".gz" : key;
    }

    private void parseObject(final String key, final S3SelectObjectWorker objectUnderTest) throws IOException {
        final S3ObjectReference s3ObjectReference = S3ObjectReference.bucketAndKey(bucket, key).build();
        objectUnderTest.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, null, null, null);
    }

    static class IntegrationTestArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<RecordsGenerator> recordsGenerators = List.of(
                    new JsonRecordsGenerator(),
                    new CsvRecordsGenerator());
            final List<Integer> numberOfRecordsList = List.of(2, 5, 25, 500, 5000);
            final List<Integer> recordsToAccumulateList = List.of(1, 100, 1000);
            final List<Boolean> booleanList = List.of(Boolean.FALSE);

            return recordsGenerators
                    .stream()
                    .flatMap(recordsGenerator -> numberOfRecordsList
                            .stream()
                            .flatMap(records -> recordsToAccumulateList
                                    .stream()
                                    .flatMap(accumulate -> booleanList
                                            .stream()
                                            .map(shouldCompress -> arguments(recordsGenerator,
                                                    records, accumulate, shouldCompress))
                                    )));
        }
    }
}
