package org.opensearch.dataprepper.plugins.source;

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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
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
    private static String bucket;
    private int recordsReceived;
    private S3ObjectGenerator s3ObjectGenerator;
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

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
        }).when(buffer).writeAll(anyCollection(), anyInt());
    }

    private ScanObjectWorker createObjectUnderTest(final RecordsGenerator recordsGenerator,
                                                   final int numberOfRecordsToAccumulate,
                                                   final String expression,
                                                   final boolean shouldCompress,
                                                   final ScanOptions scanOptions,
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
                .compressionEngine(shouldCompress ? CompressionOption.GZIP.getEngine() : CompressionOption.NONE.getEngine())
                .s3AsyncClient(s3AsyncClient)
                .eventConsumer(eventMetadataModifier)
                .expressionType("SQL")
                .serializationFormatOption(fileExtension.equals("txt") ? null :
                        S3SelectSerializationFormatOption.valueOf(fileExtension.toUpperCase()))
                .expression(isSelectEnabled ? expression : null)
                .codec(recordsGenerator.getCodec())
                .compressionType(shouldCompress ? CompressionType.GZIP : CompressionType.NONE)
                .s3SelectResponseHandlerFactory(new S3SelectResponseHandlerFactory()).build();
        return new ScanObjectWorker(s3Client,List.of(scanOptions),createObjectUnderTest(s3ObjectRequest)
        ,bucketOwnerProvider);
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

        final String includeOptionsYaml = "                include:\n" +
                "                  - "+keyPrefix+"\n" +
                "                exclude_suffix:\n" +
                "                  - .csv\n" +
                "                  - .json\n" +
                "                  - .txt\n" +
                "                  - .gz";


        final String key = getKeyString(keyPrefix, recordsGenerator, Boolean.FALSE);

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, Boolean.FALSE);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);
        final ScanOptions startTimeAndRangeScanOptions = new ScanOptions.Builder()
                .setBucket(bucket)
                .setStartDateTime(LocalDateTime.now().minusDays(1))
                .setRange(Duration.parse("P2DT10M"))
                .setS3ScanKeyPathOption(objectMapper.readValue(includeOptionsYaml, S3ScanKeyPathOption.class)).build();

        final ScanObjectWorker objectUnderTest = createObjectUnderTest(recordsGenerator,
                numberOfRecordsToAccumulate,
                expression,
                Boolean.FALSE,
                startTimeAndRangeScanOptions,
                Boolean.TRUE);
        objectUnderTest.run();
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
        final String includeOptionsYaml = "                include:\n" +
                "                  - "+keyPrefix+"\n" +
                "                exclude_suffix:\n" +
                "                  - .parquet";
        scanOptions.setS3ScanKeyPathOption(objectMapper.readValue(includeOptionsYaml, S3ScanKeyPathOption.class));
        final ScanObjectWorker scanObjectWorker = createObjectUnderTest(recordsGenerator,
                numberOfRecordsToAccumulate,
                recordsGenerator.getS3SelectExpression(),
                shouldCompress,
                scanOptions.build(),
                ThreadLocalRandom.current().nextBoolean());

        s3ObjectGenerator.write(numberOfRecords, key, recordsGenerator, shouldCompress);
        stubBufferWriter(recordsGenerator::assertEventIsCorrect, key);

        scanObjectWorker.run();

        final int expectedWrites = numberOfRecords / numberOfRecordsToAccumulate + (numberOfRecords % numberOfRecordsToAccumulate != 0 ? 1 : 0);
        verify(buffer, times(expectedWrites)).writeAll(anyCollection(), eq(TIMEOUT_IN_MILLIS));
        assertThat(recordsReceived, equalTo(numberOfRecords));
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
            final List<RecordsGenerator> recordsGenerators = List.of(new NewlineDelimitedRecordsGenerator(),new CsvRecordsGenerator(), new JsonRecordsGenerator());
            final List<Integer> numberOfRecordsList = List.of(100,5000);
            final List<Integer> recordsToAccumulateList = List.of( 100,1000);
            final List<Boolean> booleanList = List.of(Boolean.FALSE, Boolean.TRUE);
            final String bucket = System.getProperty("tests.s3source.bucket");
            final ScanOptions.Builder startTimeAndRangeScanOptions = new ScanOptions.Builder()
                    .setStartDateTime(LocalDateTime.now())
                    .setBucket(bucket)
                    .setRange(Duration.parse("P2DT10H"));
            final ScanOptions.Builder endTimeAndRangeScanOptions = new ScanOptions.Builder()
                    .setEndDateTime(LocalDateTime.now().plus(Duration.ofHours(1)))
                    .setBucket(bucket)
                    .setRange(Duration.parse("P7DT10H"));

            final ScanOptions.Builder startTimeAndEndTimeScanOptions = new ScanOptions.Builder()
                    .setStartDateTime(LocalDateTime.now().minus(Duration.ofMinutes(10)))
                    .setBucket(bucket)
                    .setEndDateTime(LocalDateTime.now().plus(Duration.ofHours(1)));

            List<ScanOptions.Builder> scanOptions = List.of(startTimeAndRangeScanOptions,endTimeAndRangeScanOptions,startTimeAndEndTimeScanOptions);
            return recordsGenerators
                    .stream()
                    .flatMap(recordsGenerator -> numberOfRecordsList
                            .stream()
                            .flatMap(records -> recordsToAccumulateList
                                    .stream()
                                    .flatMap(accumulate -> booleanList
                                                    .stream().flatMap(range -> scanOptions.stream()
                                                            .map(shouldCompress -> arguments(recordsGenerator, records,
                                                                    accumulate, range, shouldCompress))))));
        }
    }

}
