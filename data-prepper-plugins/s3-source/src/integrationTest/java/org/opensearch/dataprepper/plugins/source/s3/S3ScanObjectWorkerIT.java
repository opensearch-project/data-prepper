/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory.InMemorySourceCoordinationStore;
import org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.s3.S3ObjectDeleteWorker.S3_OBJECTS_DELETED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.s3.S3ObjectDeleteWorker.S3_OBJECTS_DELETE_FAILED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.s3.ScanObjectWorker.ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME;

@ExtendWith(MockitoExtension.class)
public class S3ScanObjectWorkerIT {

    private static final int TIMEOUT_IN_MILLIS = 200;
    private Buffer<Record<Event>> buffer;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private EventMetadataModifier eventMetadataModifier;
    private BucketOwnerProvider bucketOwnerProvider;
    private S3Client s3Client;
    private S3AsyncClient s3AsyncClient;
    private static String bucket;
    private int recordsReceived;
    private S3ObjectGenerator s3ObjectGenerator;
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private SourceCoordinator<S3SourceProgressState> sourceCoordinator;
    @Mock
    private S3SourceConfig s3SourceConfig;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private S3ScanBucketOptions s3ScanBucketOptions;
    @Mock
    private S3ScanScanOptions s3ScanScanOptions;
    @Mock
    private S3ScanSchedulingOptions s3ScanSchedulingOptions;

    final Counter acknowledgementCounter = mock(Counter.class);
    final Counter s3DeletedCounter = mock(Counter.class);
    final Counter s3DeleteFailedCounter = mock(Counter.class);


    private S3ObjectHandler createObjectUnderTest(final S3ObjectRequest s3ObjectRequest){
        if(Objects.nonNull(s3ObjectRequest.getExpression()))
            return new S3SelectObjectWorker(s3ObjectRequest);
        else
            return new S3ObjectWorker(s3ObjectRequest);
    }

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
        eventMetadataModifier = new EventMetadataModifier(S3SourceConfig.DEFAULT_METADATA_ROOT_KEY, s3SourceConfig.isDeleteS3MetadataInEvent());

        buffer = mock(Buffer.class);
        recordsReceived = 0;

        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);
        final Counter counter = mock(Counter.class);
        final DistributionSummary distributionSummary = mock(DistributionSummary.class);
        final Timer timer = new NoopTimer(new Meter.Id("test", Tags.empty(), null, null, Meter.Type.TIMER));
        lenient().when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(counter);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(counter);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter()).thenReturn(counter);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter()).thenReturn(counter);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(distributionSummary);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(distributionSummary);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(distributionSummary);
        lenient().when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(timer);
        bucketOwnerProvider = b -> Optional.empty();

        final SourceCoordinationStore inMemoryStore = new InMemorySourceCoordinationStore(new PluginSetting("in_memory", Collections.emptyMap()));
        final SourceCoordinationConfig sourceCoordinationConfig = new SourceCoordinationConfig(new PluginModel("in_memory", Collections.emptyMap()), null);
        sourceCoordinator = new LeaseBasedSourceCoordinator<>(S3SourceProgressState.class,
                inMemoryStore, sourceCoordinationConfig, "s3-test-pipeline", PluginMetrics.fromNames("source-coordinator", "s3-test-pipeline"));
        sourceCoordinator.initialize();
    }

    private void stubBufferWriter(final Consumer<Event> additionalEventAssertions, final String key) throws Exception {
        lenient().doAnswer(a -> {
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
        }).when(buffer).writeAll(anyCollection(), anyInt());
    }

    private ScanObjectWorker createObjectUnderTest(final RecordsGenerator recordsGenerator,
                                                   final int numberOfRecordsToAccumulate,
                                                   final String expression,
                                                   final boolean shouldCompress,
                                                   final List<ScanOptions> scanOptions,
                                                   final boolean isSelectEnabled) throws JsonProcessingException {
        final String fileExtension = recordsGenerator.getFileExtension();
        final String csvYaml ="file_header_info: none";
        final S3SelectCSVOption S3SelectCSVOption = objectMapper.readValue(csvYaml, S3SelectCSVOption.class);
        final S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer, numberOfRecordsToAccumulate,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3SelectCSVOption(S3SelectCSVOption)
                .s3Client(s3Client)
                .s3SelectJsonOption(new S3SelectJsonOption())
                .compressionOption(shouldCompress ? CompressionOption.GZIP : CompressionOption.NONE)
                .s3AsyncClient(s3AsyncClient)
                .eventConsumer(eventMetadataModifier)
                .expressionType("SQL")
                .serializationFormatOption(fileExtension.equals("txt") ? null :
                        S3SelectSerializationFormatOption.valueOf(fileExtension.toUpperCase()))
                .expression(isSelectEnabled ? expression : null)
                .codec(recordsGenerator.getCodec())
                .compressionType(shouldCompress ? CompressionType.GZIP : CompressionType.NONE)
                .s3SelectResponseHandlerFactory(new S3SelectResponseHandlerFactory()).build();

        when(pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME)).thenReturn(acknowledgementCounter);
        when(pluginMetrics.counter(S3_OBJECTS_DELETED_METRIC_NAME)).thenReturn(s3DeletedCounter);
        when(pluginMetrics.counter(S3_OBJECTS_DELETE_FAILED_METRIC_NAME)).thenReturn(s3DeleteFailedCounter);
        S3ObjectDeleteWorker s3ObjectDeleteWorker = new S3ObjectDeleteWorker(s3Client, pluginMetrics);

        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        when(s3ScanScanOptions.getSchedulingOptions()).thenReturn(s3ScanSchedulingOptions);
        lenient().when(s3ScanSchedulingOptions.getInterval()).thenReturn(Duration.ofHours(1));
        lenient().when(s3ScanSchedulingOptions.getCount()).thenReturn(1);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(executor);

        return new ScanObjectWorker(s3Client, scanOptions, createObjectUnderTest(s3ObjectRequest)
        ,bucketOwnerProvider, sourceCoordinator, s3SourceConfig, acknowledgementSetManager, s3ObjectDeleteWorker, 30000, pluginMetrics);
    }

    @ParameterizedTest
    @CsvSource({"25,10,select * from s3Object limit 25",
            "100,25,select * from s3Object limit 100",
            "50000,25,select * from s3Object limit 50000",
            "100000,50,select * from s3Object limit 100000",
            "200000,200,select * from s3Object limit 200000",
            "300000,300,select * from s3Object limit 300000"})
    void parseS3Object_parquet_correctly_with_bucket_scan_and_loads_data_into_Buffer(
            final int numberOfRecords,
            final int numberOfRecordsToAccumulate,
            final String expression) throws Exception {
        final RecordsGenerator recordsGenerator = new ParquetRecordsGenerator();
        final String keyPrefix = "s3source/s3-scan/" + recordsGenerator.getFileExtension() + "/" + Instant.now().toEpochMilli();

        final String buketOptionYaml = "name: " + bucket + "\n" +
                "filter:\n" +
                "  include_prefix:\n" +
                "    - " + keyPrefix + "\n" +
                "  exclude_suffix:\n" +
                "    - .csv\n" +
                "    - .json\n" +
                "    - .txt\n" +
                "    - .gz";

        final String key = getKeyString(keyPrefix, recordsGenerator, Boolean.FALSE);

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, Boolean.FALSE);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);
        final ScanOptions startTimeAndRangeScanOptions = new ScanOptions.Builder()
                .setBucketOption(objectMapper.readValue(buketOptionYaml, S3ScanBucketOption.class))
                .setStartDateTime(LocalDateTime.now().minusDays(1))
                .setEndDateTime(LocalDateTime.now().plus(Duration.ofMinutes(5)))
                .build();

        final ScanObjectWorker objectUnderTest = createObjectUnderTest(recordsGenerator,
                numberOfRecordsToAccumulate,
                expression,
                Boolean.FALSE,
                List.of(startTimeAndRangeScanOptions),
                Boolean.TRUE);

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.submit(objectUnderTest::run);

        await().atMost(Duration.ofSeconds(30)).until(() -> waitForAllRecordsToBeProcessed(numberOfRecords));
        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);

        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));

        assertThat(recordsReceived, equalTo(numberOfRecords));
    }

    @ParameterizedTest
    @ArgumentsSource(S3ScanObjectWorkerIT.IntegrationTestArguments.class)
    void parseS3Object_correctly_with_bucket_scan_and_loads_data_into_Buffer(
            final RecordsGenerator recordsGenerator,
            final int numberOfRecords,
            final int numberOfRecordsToAccumulate,
            final boolean shouldCompress,
            final ScanOptions.Builder scanOptions) throws Exception {
        String keyPrefix = "s3source/s3-scan/" + recordsGenerator.getFileExtension() + "/" + Instant.now().toEpochMilli();
        final String key = getKeyString(keyPrefix,recordsGenerator, shouldCompress);
        final String buketOptionYaml = "name: " + bucket + "\n" +
                "filter:\n" +
                "  include_prefix:\n" +
                "    - " + keyPrefix;
        scanOptions.setBucketOption(objectMapper.readValue(buketOptionYaml, S3ScanBucketOption.class));

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, shouldCompress);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest(recordsGenerator,
                numberOfRecordsToAccumulate,
                recordsGenerator.getS3SelectExpression(),
                shouldCompress,
                List.of(scanOptions.build()),
                ThreadLocalRandom.current().nextBoolean());

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, shouldCompress);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);

        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.submit(scanObjectWorker::run);

        await().atMost(Duration.ofSeconds(30)).until(() -> waitForAllRecordsToBeProcessed(numberOfRecords));

        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));
        assertThat(recordsReceived, equalTo(numberOfRecords));

        executorService.shutdownNow();
    }

    @Disabled("TODO: Implement logic to get ack with S3 scan test setup")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void parseS3Object_correctly_with_bucket_scan_and_loads_data_into_Buffer_and_deletes_s3_object(final boolean deleteS3Objects) throws Exception {
        final RecordsGenerator recordsGenerator = new NewlineDelimitedRecordsGenerator();
        final boolean shouldCompress = true;
        final int numberOfRecords = 100;
        final int numberOfRecordsToAccumulate = 50;

        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(deleteS3Objects);
        String keyPrefix = "s3source/s3-scan/" + recordsGenerator.getFileExtension() + "/" + Instant.now().toEpochMilli();
        final String key = getKeyString(keyPrefix, recordsGenerator, shouldCompress);
        final String buketOptionYaml = "name: " + bucket + "\n" +
                "filter:\n" +
                "  include_prefix:\n" +
                "    - " + keyPrefix;

        final ScanOptions.Builder startTimeAndEndTimeScanOptions = ScanOptions.builder()
                .setStartDateTime(LocalDateTime.now().minus(Duration.ofMinutes(10)))
                .setEndDateTime(LocalDateTime.now().plus(Duration.ofHours(1)));

        List<ScanOptions.Builder> scanOptions = List.of(startTimeAndEndTimeScanOptions);
        final ScanOptions.Builder s3ScanOptionsBuilder = scanOptions.get(0).setBucketOption(objectMapper.readValue(buketOptionYaml, S3ScanBucketOption.class));

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, shouldCompress);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest(recordsGenerator,
                numberOfRecordsToAccumulate,
                recordsGenerator.getS3SelectExpression(),
                shouldCompress,
                List.of(s3ScanOptionsBuilder.build()),
                ThreadLocalRandom.current().nextBoolean());


        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate;

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.submit(scanObjectWorker::run);


        await().atMost(Duration.ofSeconds(60)).until(() -> waitForAllRecordsToBeProcessed(numberOfRecords));

        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));
        assertThat(recordsReceived, equalTo(numberOfRecords));

        // wait for S3 objects to be deleted to verify metrics
        Thread.sleep(500);

        if (deleteS3Objects)
            verify(s3DeletedCounter, times(1)).increment();
        verifyNoMoreInteractions(s3DeletedCounter);
        verifyNoInteractions(s3DeleteFailedCounter);

        executorService.shutdownNow();
    }

    @Test
    public void processS3Object_test_metadata_only() throws Exception {
        String keyPrefix = "s3source/s3-scan/metadataTest/" + Instant.now().toEpochMilli();
        final String bucketOptionYaml = "name: " + bucket + "\n" +
                "data_selection: metadata_only\n"+
                "filter:\n" +
                "  include_prefix:\n" +
                "    - " + keyPrefix;
        S3ScanBucketOption s3BucketOption = objectMapper.readValue(bucketOptionYaml, S3ScanBucketOption.class);
        final ScanOptions startTimeAndRangeScanOptions = new ScanOptions.Builder()
                .setBucketOption(s3BucketOption)
                .setStartDateTime(LocalDateTime.now().minusDays(1))
                .setEndDateTime(LocalDateTime.now().plus(Duration.ofMinutes(5)))
                .build();
        recordsReceived = 0;
        lenient().doAnswer(a -> {
            final Collection<Record<Event>> recordsCollection = a.getArgument(0);
            recordsReceived += recordsCollection.size();
            return null;
        }).when(buffer).writeAll(anyCollection(), anyInt());
        when(s3ScanBucketOptions.getS3ScanBucketOption()).thenReturn(s3BucketOption);
        when(s3ScanScanOptions.getBuckets()).thenReturn(List.of(s3ScanBucketOptions));

        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        when(s3ScanScanOptions.getSchedulingOptions()).thenReturn(s3ScanSchedulingOptions);
        lenient().when(s3ScanSchedulingOptions.getInterval()).thenReturn(Duration.ofHours(1));
        lenient().when(s3ScanSchedulingOptions.getCount()).thenReturn(1);

        int numberOfObjects = 10;
        int numberOfObjectsToAccumulate = 1;
        final RecordsGenerator recordsGenerator = new NewlineDelimitedRecordsGenerator();
        final List<String> keyList = new ArrayList<>();
        for (int i = 0; i < numberOfObjects; i++) {
            final String key = keyPrefix +"/test"+i+"."+recordsGenerator.getFileExtension();
            keyList.add(key);
            s3ObjectGenerator.write(1, key, recordsGenerator, Boolean.FALSE);
        }
        final S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer, numberOfObjectsToAccumulate,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3Client(s3Client)
                .build();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(executor);
        final ScanObjectWorker objectUnderTest = createObjectUnderTest(recordsGenerator,
                1,
                null,
                Boolean.FALSE,
                List.of(startTimeAndRangeScanOptions),
                Boolean.TRUE);

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.submit(objectUnderTest::run);

        await().atMost(Duration.ofSeconds(30)).until(() -> waitForAllRecordsToBeProcessed(numberOfObjects));
        final int expectedWrites = numberOfObjects / numberOfObjectsToAccumulate + (numberOfObjects % numberOfObjectsToAccumulate != 0 ? 1 : 0);
        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));

        assertThat(recordsReceived, equalTo(numberOfObjects));

        for (String deleteKey: keyList) {
            final DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(deleteKey).build();
            s3Client.deleteObject(deleteObjectRequest);
        }

    }

    @Test
    public void processS3Object_test_multiple_dataselections_on_multiple_buckets() throws Exception {
        String keyPrefix = "s3source/s3-scan/dataMetadataTest/" + Instant.now().toEpochMilli();
        final String bucketOptionYaml = "name: " + bucket + "\n" +
                "filter:\n" +
                "  include_prefix:\n" +
                "    - " + keyPrefix;
        String keyPrefix2 = "s3source/s3-scan/dataTest/" + Instant.now().toEpochMilli();
        final String bucketOptionYaml2 = "name: " + bucket + "\n" +
                "filter:\n" +
                "  include_prefix:\n" +
                "    - " + keyPrefix2;
        S3ScanBucketOption s3BucketOption1 = objectMapper.readValue(bucketOptionYaml, S3ScanBucketOption.class);
        final ScanOptions startTimeAndRangeScanOptions = new ScanOptions.Builder()
                .setBucketOption(s3BucketOption1)
                .setStartDateTime(LocalDateTime.now().minusDays(1))
                .setEndDateTime(LocalDateTime.now().plus(Duration.ofMinutes(5)))
                .build();
        S3ScanBucketOption s3BucketOption2 = objectMapper.readValue(bucketOptionYaml2, S3ScanBucketOption.class);
        final ScanOptions startTimeAndRangeScanOptions2 = new ScanOptions.Builder()
                .setBucketOption(s3BucketOption2)
                .setStartDateTime(LocalDateTime.now().minusDays(1))
                .setEndDateTime(LocalDateTime.now().plus(Duration.ofMinutes(5)))
                .build();
        recordsReceived = 0;
        lenient().doAnswer(a -> {
            final Collection<Record<Event>> recordsCollection = a.getArgument(0);
            recordsReceived += recordsCollection.size();
            return null;
        }).when(buffer).writeAll(anyCollection(), anyInt());
        when(s3ScanBucketOptions.getS3ScanBucketOption()).thenReturn(s3BucketOption1);

        S3ScanBucketOptions s3ScanBucketOptions2 = mock(S3ScanBucketOptions.class);
        when(s3ScanBucketOptions2.getS3ScanBucketOption()).thenReturn(s3BucketOption2);
        when(s3ScanScanOptions.getBuckets()).thenReturn(List.of(s3ScanBucketOptions, s3ScanBucketOptions2));

        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        when(s3ScanScanOptions.getSchedulingOptions()).thenReturn(s3ScanSchedulingOptions);
        lenient().when(s3ScanSchedulingOptions.getInterval()).thenReturn(Duration.ofHours(1));
        lenient().when(s3ScanSchedulingOptions.getCount()).thenReturn(1);

        int numberOfMetadataObjects = 10;
        int numberOfObjectsToAccumulate = 1;
        final RecordsGenerator recordsGenerator = new NewlineDelimitedRecordsGenerator();
        final List<String> keyList = new ArrayList<>();
        for (int i = 0; i < numberOfMetadataObjects; i++) {
            final String key = keyPrefix +"/test"+i+"."+recordsGenerator.getFileExtension();
            keyList.add(key);
            s3ObjectGenerator.write(1, key, recordsGenerator, Boolean.FALSE);
        }
    
        final String key2 = keyPrefix2 +"/datatest."+recordsGenerator.getFileExtension();
        int numberOfRecordsInObject = 100;
        s3ObjectGenerator.write(numberOfRecordsInObject, key2, recordsGenerator, Boolean.FALSE);
        int numberOfObjects = numberOfMetadataObjects + numberOfRecordsInObject;

        final S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer, numberOfObjectsToAccumulate,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3Client(s3Client)
                .build();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(executor);
        final ScanObjectWorker objectUnderTest = createObjectUnderTest(recordsGenerator,
                1,
                null,
                Boolean.FALSE,
                List.of(startTimeAndRangeScanOptions, startTimeAndRangeScanOptions2),
                Boolean.TRUE);

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.submit(objectUnderTest::run);

        await().atMost(Duration.ofSeconds(30)).until(() -> waitForAllRecordsToBeProcessed(numberOfObjects));
        final int expectedWrites = numberOfObjects / numberOfObjectsToAccumulate + (numberOfObjects % numberOfObjectsToAccumulate != 0 ? 1 : 0);
        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));

        assertThat(recordsReceived, equalTo(numberOfObjects));
        for (String deleteKey: keyList) {
            final DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(deleteKey).build();
            s3Client.deleteObject(deleteObjectRequest);
        }

    }


    private String getKeyString(final String keyPrefix ,
                                final RecordsGenerator recordsGenerator,
                                final boolean shouldCompress) {
        String key = keyPrefix + "/" + Instant.now().toEpochMilli()  + "." + recordsGenerator.getFileExtension();
        return shouldCompress ? key + ".gz" : key;
    }

    static class IntegrationTestArguments implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<RecordsGenerator> recordsGenerators = List.of(
                    new NewlineDelimitedRecordsGenerator(),
                    new CsvRecordsGenerator(),
                    new JsonRecordsGenerator(),
                    new ParquetRecordsGenerator());
            final List<Integer> numberOfRecordsList = List.of(100,5000);
            final List<Integer> recordsToAccumulateList = List.of( 100);
            final List<Boolean> booleanList = List.of(Boolean.TRUE);

            final ScanOptions.Builder startTimeScanOptions = ScanOptions.builder()
                    .setStartDateTime(LocalDateTime.now());
            final ScanOptions.Builder endTimeScanOptions = ScanOptions.builder()
                    .setEndDateTime(LocalDateTime.now().plus(Duration.ofHours(1)));
            final ScanOptions.Builder startTimeAndEndTimeScanOptions = ScanOptions.builder()
                    .setStartDateTime(LocalDateTime.now().minus(Duration.ofMinutes(10)))
                    .setEndDateTime(LocalDateTime.now().plus(Duration.ofHours(1)));
            final ScanOptions.Builder rangeScanOptions = ScanOptions.builder()
                    .setRange(Duration.parse("P7DT10H"));

            List<ScanOptions.Builder> scanOptions = List.of(startTimeScanOptions, endTimeScanOptions, startTimeAndEndTimeScanOptions, rangeScanOptions);
            return recordsGenerators
                    .stream()
                    .flatMap(recordsGenerator -> numberOfRecordsList
                            .stream()
                            .flatMap(records -> recordsToAccumulateList
                                    .stream()
                                    .flatMap(accumulate -> booleanList
                                            .stream()
                                            .flatMap(shouldCompress -> scanOptions.stream()
                                                    .map(scanOptionsBuilder -> arguments(recordsGenerator, records,
                                                                    accumulate, shouldCompress && recordsGenerator.canCompress(), scanOptionsBuilder))))));
        }
    }

    public boolean waitForAllRecordsToBeProcessed(final int numberOfRecords) {
        return recordsReceived == numberOfRecords;
    }
}
