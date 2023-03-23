package org.opensearch.dataprepper.plugins.source;

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
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.compression.GZipCompressionEngine;
import org.opensearch.dataprepper.plugins.source.compression.NoneCompressionEngine;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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

public class S3ScanObjectWorkerIT {
    private static final int TIMEOUT_IN_MILLIS = 200;
    private Buffer<Record<Event>> buffer;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private EventMetadataModifier eventMetadataModifier;
    private BucketOwnerProvider bucketOwnerProvider;
    private S3Client s3Client;
    private S3AsyncClient s3AsyncClient;
    private String bucket;
    private int recordsReceived;
    private S3ObjectGenerator s3ObjectGenerator;

    @BeforeEach
    void setUp() {
        s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        bucket = System.getProperty("tests.s3source.bucket");
        s3ObjectGenerator = new S3ObjectGenerator(s3Client, bucket);
        eventMetadataModifier = new EventMetadataModifier(S3SourceConfig.DEFAULT_METADATA_ROOT_KEY);

        buffer = mock(Buffer.class);
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

    private ScanObjectWorker createObjectUnderTest(final RecordsGenerator recordsGenerator, final int numberOfRecordsToAccumulate,
                                                   final CompressionEngine compressionEngine,
                                                   final List<String> keys,
                                                   final String query,
                                                   final boolean shouldCompress,
                                                   final String range) {
        final S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer, numberOfRecordsToAccumulate,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics).bucketOwnerProvider(bucketOwnerProvider)
                .eventConsumer(eventMetadataModifier).codec(recordsGenerator.getCodec()).s3Client(s3Client).s3AsyncClient(s3AsyncClient)
                .compressionEngine(compressionEngine).build();
        final ScanOptionsBuilder scanOptionsBuilder = new ScanOptionsBuilder().
                setStartDate(LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES).plusHours(10).toString()).
                setRange(range).
                setBucket(bucket).
                setQuery(query.isEmpty() ? null : query).
                setSerializationFormatOption(S3SelectSerializationFormatOption.valueOf(recordsGenerator.getFileExtension().toUpperCase())).
                setKeys(keys).
                setCodec(recordsGenerator.getCodec()).setCsvHeaderInfo(FileHeaderInfo.NONE).
                setCompressionOption(shouldCompress ? CompressionOption.GZIP : CompressionOption.NONE).
                setCompressionType(shouldCompress ? CompressionType.GZIP : CompressionType.NONE);
        return new ScanObjectWorker(s3ObjectRequest, List.of(scanOptionsBuilder));
    }

    @ParameterizedTest
    @ArgumentsSource(S3ScanObjectWorkerIT.IntegrationTestArguments.class)
    void parseS3Object_correctly_loads_data_into_Buffer(
            final RecordsGenerator recordsGenerator,
            final int numberOfRecords,
            final int numberOfRecordsToAccumulate,
            final boolean shouldCompress,
            final String range) throws Exception {
        final String key = getKeyString(recordsGenerator, numberOfRecords, shouldCompress);

        final CompressionEngine compressionEngine = shouldCompress ? new GZipCompressionEngine() : new NoneCompressionEngine();
        final ScanObjectWorker scanObjectWorker = createObjectUnderTest(recordsGenerator,
                numberOfRecordsToAccumulate, compressionEngine, List.of(key), recordsGenerator.getQueryStatement(),
                shouldCompress, range);

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, shouldCompress);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);

        scanObjectWorker.run();

        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);
        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));
        assertThat(recordsReceived, equalTo(numberOfRecords));
    }

    private String getKeyString(final RecordsGenerator recordsGenerator, final int numberOfRecords, final boolean shouldCompress) {
        String key = "s3source/s3/" + numberOfRecords + "_" + Instant.now().toEpochMilli() + "." + recordsGenerator.getFileExtension();
        return shouldCompress ? key + ".gz" : key;
    }

    static class IntegrationTestArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<RecordsGenerator> recordsGenerators = List.of(new CsvRecordsGenerator(), new JsonRecordsGenerator());
            final List<Integer> numberOfRecordsList = List.of(5000);
            final List<Integer> recordsToAccumulateList = List.of( 1000);
            final List<Boolean> booleanList = List.of(Boolean.FALSE, Boolean.TRUE);
            final List<String> rangeList = List.of("2d", "1w", "3m", "1y");
            return recordsGenerators
                    .stream()
                    .flatMap(recordsGenerator -> numberOfRecordsList
                            .stream()
                            .flatMap(records -> recordsToAccumulateList
                                    .stream()
                                    .flatMap(accumulate -> booleanList
                                                    .stream().flatMap(range -> rangeList.stream()
                                                            .map(shouldCompress -> arguments(recordsGenerator, records,
                                                                    accumulate, range, shouldCompress))))));
        }
    }

}
