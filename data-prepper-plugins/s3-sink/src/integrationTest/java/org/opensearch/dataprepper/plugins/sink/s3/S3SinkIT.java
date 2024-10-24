/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AggregateThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ThresholdOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3SinkIT {
    private static final Logger LOG = LoggerFactory.getLogger(S3SinkIT.class);
    private static final Random RANDOM = new Random();

    private static List<String> reusableRandomStrings;

    @Mock
    private PluginSetting pluginSetting;
    @Mock(stubOnly = true)
    private S3SinkConfig s3SinkConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private SinkContext sinkContext;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock(stubOnly = true)
    private ThresholdOptions thresholdOptions;
    @Mock(stubOnly = true)
    private ObjectKeyOptions objectKeyOptions;

    @Mock(stubOnly = true)
    private ExpressionEvaluator expressionEvaluator;
    private String s3region;
    private String bucketName;
    private S3Client s3Client;

    @TempDir
    private File s3FileLocation;
    private static String pathPrefixForTestSuite;

    @BeforeAll
    static void setUpAll() {
        LocalDateTime now = LocalDateTime.now();
        String datePart = LocalDate.from(now).format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        String time = LocalTime.from(now).toString();
        pathPrefixForTestSuite = datePart + "/" + time + "-" + UUID.randomUUID() + "/";

        int totalRandomStrings = 1_000;
        reusableRandomStrings = new ArrayList<>(totalRandomStrings);
        for (int i = 0; i < totalRandomStrings; i++) {
            reusableRandomStrings.add(UUID.randomUUID().toString());
        }


    }

    @BeforeEach
    void setUp() {
        when(pluginSetting.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        when(pluginSetting.getName()).thenReturn("s3");

        s3region = System.getProperty("tests.s3sink.region");

        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();

        final Region region = Region.of(s3region);
        bucketName = System.getProperty("tests.s3sink.bucket");

        when(s3SinkConfig.getBucketName()).thenReturn(bucketName);
        when(objectKeyOptions.getNamePattern()).thenReturn("events-%{yyyy-MM-dd'T'hh-mm-ss}");
        when(s3SinkConfig.getObjectKeyOptions()).thenReturn(objectKeyOptions);

        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofDays(1));
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("1gb"));

        final PluginModel pluginModel = mock(PluginModel.class);
        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());

        final AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);

        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

        s3Client = S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(region)
                .build();

        when(expressionEvaluator.isValidFormatExpression(anyString())).thenReturn(true);

        when(s3SinkConfig.getDefaultBucket()).thenReturn(null);
        when(s3SinkConfig.getBucketOwners()).thenReturn(null);
        when(s3SinkConfig.getDefaultBucketOwner()).thenReturn(null);
    }

    private S3Sink createObjectUnderTest() {
        return new S3Sink(pluginSetting, s3SinkConfig, pluginFactory, sinkContext, awsCredentialsSupplier, expressionEvaluator);
    }

    @ParameterizedTest
    @ArgumentsSource(BufferCombinationsArguments.class)
    @ArgumentsSource(LargerBufferCombinationsArguments.class)
    @ArgumentsSource(CodecArguments.class)
    void test(final OutputScenario outputScenario,
              final BufferScenario bufferScenario,
              final CompressionScenario compressionScenario,
              final SizeCombination sizeCombination) throws IOException {

        BufferTypeOptions bufferTypeOptions = bufferScenario.getBufferType();
        String testRun = outputScenario + "-" + bufferTypeOptions + "-" + compressionScenario + "-" + sizeCombination.getBatchSize() + "-" + sizeCombination.getNumberOfBatches();
        final String pathPrefix = pathPrefixForTestSuite + testRun;
        when(objectKeyOptions.getPathPrefix()).thenReturn(pathPrefix + "/");

        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenReturn(outputScenario.getCodec());
        when(s3SinkConfig.getBufferType()).thenReturn(bufferTypeOptions);
        when(s3SinkConfig.getCompression()).thenReturn(compressionScenario.getCompressionOption());
        int expectedTotalSize = sizeCombination.getTotalSize();
        when(thresholdOptions.getEventCount()).thenReturn(expectedTotalSize);

        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(anyString()))
                .thenReturn(Collections.emptyList());
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(anyString()))
                .thenReturn(Collections.emptyList());

        final S3Sink objectUnderTest = createObjectUnderTest();

        final int maxEventDataToSample = 2000;
        final List<Map<String, Object>> sampleEventData = new ArrayList<>(maxEventDataToSample);
        for (int batchNumber = 0; batchNumber < sizeCombination.getNumberOfBatches(); batchNumber++) {
            final int currentBatchNumber = batchNumber;
            final List<Record<Event>> events = IntStream.range(0, sizeCombination.getBatchSize())
                    .mapToObj(sequence -> generateEventData((currentBatchNumber + 1) * (sequence + 1)))
                    .peek(data -> {
                        if (sampleEventData.size() < maxEventDataToSample)
                            sampleEventData.add(data);
                    })
                    .map(this::generateTestEvent)
                    .map(Record::new)
                    .collect(Collectors.toList());

            LOG.debug("Writing batch {} with size {}.", currentBatchNumber, events.size());
            objectUnderTest.doOutput(events);
        }

        LOG.info("Listing S3 path prefix: {}", pathPrefix);

        final ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(pathPrefix + "/")
                .build());

        assertThat(listObjectsResponse.contents(), notNullValue());
        assertThat(listObjectsResponse.contents().size(), equalTo(1));

        final S3Object s3Object = listObjectsResponse.contents().get(0);

        final File target = new File(s3FileLocation, testRun + ".original");

        LOG.info("Downloading S3 object to local file {}.", target);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Object.key())
                .build();
        s3Client.getObject(getObjectRequest, target.toPath());

        File actualContentFile = decompressFileIfNecessary(outputScenario, compressionScenario, testRun, target);

        LOG.info("Validating output. totalSize={}; sampleDataSize={}", expectedTotalSize, sampleEventData.size());
        outputScenario.validate(expectedTotalSize, sampleEventData, actualContentFile, compressionScenario);
    }

    @ParameterizedTest
    @ArgumentsSource(OutputScenarioArguments.class)
    void testWithDynamicGroups(final OutputScenario outputScenario) throws IOException {
        final BufferScenario bufferScenario = new InMemoryBufferScenario();
        final CompressionScenario compressionScenario = new NoneCompressionScenario();
        final SizeCombination sizeCombination = SizeCombination.MEDIUM_SMALLER;

        BufferTypeOptions bufferTypeOptions = bufferScenario.getBufferType();
        String testRun = "grouping-" + outputScenario + "-" + bufferTypeOptions + "-" + compressionScenario + "-" + sizeCombination.getBatchSize() + "-" + sizeCombination.getNumberOfBatches();

        final String staticPrefix = "s3-sink-grouping-integration-test/" + UUID.randomUUID() + "/";
        final String pathPrefix = staticPrefix +  "folder-${/sequence}/";
        final List<String> dynamicKeys = new ArrayList<>();
        dynamicKeys.add("/sequence");

        when(objectKeyOptions.getPathPrefix()).thenReturn(pathPrefix);

        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(objectKeyOptions.getPathPrefix()))
                .thenReturn(Collections.emptyList());
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(objectKeyOptions.getPathPrefix()))
                .thenReturn(dynamicKeys);
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(objectKeyOptions.getNamePattern()))
                .thenReturn(Collections.emptyList());
        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(objectKeyOptions.getNamePattern()))
                .thenReturn(Collections.emptyList());

        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenAnswer(invocation -> outputScenario.getCodec());
        when(s3SinkConfig.getBufferType()).thenReturn(bufferTypeOptions);
        when(s3SinkConfig.getCompression()).thenReturn(compressionScenario.getCompressionOption());
        int expectedTotalSize = sizeCombination.getTotalSize();
        when(thresholdOptions.getEventCount()).thenReturn(expectedTotalSize / sizeCombination.getBatchSize());

        final S3Sink objectUnderTest = createObjectUnderTest();

        final int maxEventDataToSample = 2000;
        final List<Map<String, Object>> sampleEventData = new ArrayList<>(maxEventDataToSample);
        for (int batchNumber = 0; batchNumber < sizeCombination.getNumberOfBatches(); batchNumber++) {
            final int currentBatchNumber = batchNumber;
            final List<Record<Event>> events = IntStream.range(0, sizeCombination.getBatchSize())
                    .mapToObj(this::generateEventData)
                    .peek(data -> {
                        if (sampleEventData.size() < maxEventDataToSample)
                            sampleEventData.add(data);
                    })
                    .map(this::generateTestEvent)
                    .map(Record::new)
                    .collect(Collectors.toList());

            LOG.debug("Writing dynamic batch {} with size {}.", currentBatchNumber, events.size());
            objectUnderTest.doOutput(events);
        }

        for (int folderNumber = 0; folderNumber < 100; folderNumber++) {
            LOG.info("Listing S3 path prefix: {}", staticPrefix + "folder-" + folderNumber + "/");

            final ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(staticPrefix + "folder-" + folderNumber + "/")
                    .build());

            assertThat(listObjectsResponse.contents(), notNullValue());
            assertThat(listObjectsResponse.contents().size(), equalTo(1));

            final S3Object s3Object = listObjectsResponse.contents().get(0);

            final File target = new File(s3FileLocation, "folder-" + folderNumber + ".original");

            LOG.info("Downloading S3 object to local file {}.", target);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Object.key())
                    .build();
            s3Client.getObject(getObjectRequest, target.toPath());

            File actualContentFile = decompressFileIfNecessary(outputScenario, compressionScenario, testRun, target);

            LOG.info("Validating output. totalSize={}; sampleDataSize={}", expectedTotalSize, sampleEventData.size());
            outputScenario.validateDynamicPartition(expectedTotalSize / sizeCombination.getBatchSize(), folderNumber, actualContentFile, compressionScenario);
        }
    }

    @Test
    void testWithDynamicGroupsAndAggregateThreshold() throws IOException {
        final BufferScenario bufferScenario = new InMemoryBufferScenario();
        final CompressionScenario compressionScenario = new NoneCompressionScenario();
        final NdjsonOutputScenario outputScenario = new NdjsonOutputScenario();
        final SizeCombination sizeCombination = SizeCombination.MEDIUM_SMALLER;

        BufferTypeOptions bufferTypeOptions = bufferScenario.getBufferType();
        String testRun = "grouping-" + outputScenario + "-" + bufferTypeOptions + "-" + compressionScenario + "-" + sizeCombination.getBatchSize() + "-" + sizeCombination.getNumberOfBatches();

        final String staticPrefix = "s3-sink-grouping-integration-test/aggregate-threshold-" + UUID.randomUUID() + "/";
        final String pathPrefix = staticPrefix +  "folder-${/sequence}/";
        final List<String> dynamicKeys = new ArrayList<>();
        dynamicKeys.add("/sequence");

        final AggregateThresholdOptions aggregateThresholdOptions = mock(AggregateThresholdOptions.class);
        when(aggregateThresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("100kb"));
        when(s3SinkConfig.getAggregateThresholdOptions()).thenReturn(aggregateThresholdOptions);

        when(objectKeyOptions.getPathPrefix()).thenReturn(pathPrefix);

        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(objectKeyOptions.getPathPrefix()))
                .thenReturn(Collections.emptyList());
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(objectKeyOptions.getPathPrefix()))
                .thenReturn(dynamicKeys);
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(objectKeyOptions.getNamePattern()))
                .thenReturn(Collections.emptyList());
        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(objectKeyOptions.getNamePattern()))
                .thenReturn(Collections.emptyList());

        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenAnswer(invocation -> outputScenario.getCodec());
        when(s3SinkConfig.getBufferType()).thenReturn(bufferTypeOptions);
        when(s3SinkConfig.getCompression()).thenReturn(compressionScenario.getCompressionOption());
        int expectedTotalSize = sizeCombination.getTotalSize();

        // This will not be hit since these are grouped dynamically
        when(thresholdOptions.getEventCount()).thenReturn(expectedTotalSize);

        final S3Sink objectUnderTest = createObjectUnderTest();

        final int maxEventDataToSample = 2000;
        final List<Map<String, Object>> sampleEventData = new ArrayList<>(maxEventDataToSample);
        for (int batchNumber = 0; batchNumber < sizeCombination.getNumberOfBatches(); batchNumber++) {
            final int currentBatchNumber = batchNumber;
            final List<Record<Event>> events = IntStream.range(0, sizeCombination.getBatchSize())
                    .mapToObj(this::generateEventData)
                    .peek(data -> {
                        if (sampleEventData.size() < maxEventDataToSample)
                            sampleEventData.add(data);
                    })
                    .map(this::generateTestEvent)
                    .map(Record::new)
                    .collect(Collectors.toList());

            LOG.debug("Writing dynamic batch {} with size {}.", currentBatchNumber, events.size());
            objectUnderTest.doOutput(events);
        }

        for (int folderNumber = 0; folderNumber < 100; folderNumber++) {
            LOG.info("Listing S3 path prefix: {}", staticPrefix + "folder-" + folderNumber + "/");

            final ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(staticPrefix + "folder-" + folderNumber + "/")
                    .build());

            assertThat(listObjectsResponse.contents(), notNullValue());
            assertThat(listObjectsResponse.contents().size(), greaterThanOrEqualTo(1));

            int objectIndex = 0;
            for (final S3Object s3Object : listObjectsResponse.contents()) {
                final File target = new File(s3FileLocation, "folder-" + folderNumber + "-" + objectIndex + ".original");

                LOG.info("Downloading S3 object to local file {}.", target);

                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Object.key())
                        .build();
                s3Client.getObject(getObjectRequest, target.toPath());

                File actualContentFile = decompressFileIfNecessary(outputScenario, compressionScenario, testRun, target);

                // Since data is evenly distributed between all groups, and flush_capacity_ratio is 0
                // all objects should be less than 10 kb (100 groups * 10 kb = aggregate threshold of 1 mb)
                assertThat(actualContentFile.length(), lessThan(10_000L));
                LOG.info("Validating output. object size is {}", actualContentFile.length());
                outputScenario.validateDynamicPartition(-1, folderNumber, actualContentFile, compressionScenario);
                objectIndex++;
            }
        }
    }

    private File decompressFileIfNecessary(OutputScenario outputScenario, CompressionScenario compressionScenario, String pathPrefix, File target) throws IOException {

        if (outputScenario.isCompressionInternal() || !compressionScenario.requiresDecompression())
            return target;

        File actualContentFile = new File(s3FileLocation, pathPrefix + ".content");
        IOUtils.copy(
                compressionScenario.decompressingInputStream(new FileInputStream(target)),
                new FileOutputStream(actualContentFile));

        return actualContentFile;
    }

    private Event generateTestEvent(final Map<String, Object> eventData) {
        final EventMetadata defaultEventMetadata = DefaultEventMetadata.builder()
                .withEventType(EventType.LOG.toString())
                .build();
        return JacksonEvent.builder()
                .withData(eventData)
                .withEventMetadata(defaultEventMetadata)
                .build();
    }

    private Map<String, Object> generateEventData(final int sequence) {
        final Map<String, Object> eventDataMap = new LinkedHashMap<>();
        eventDataMap.put("sequence", sequence);
        eventDataMap.put("id", UUID.randomUUID().toString());
        for (int i = 0; i < 2; i++) {
            eventDataMap.put("field" + i, reusableRandomString());
            eventDataMap.put("float" + i, (float) i * 1.5 * sequence);
        }
        for (int i = 0; i < 2; i++) {
            eventDataMap.put("list" + i,
                    List.of(reusableRandomString(), reusableRandomString(), reusableRandomString()));
        }
        return eventDataMap;
    }


    /**
     * These tests focus on various size combinations for various buffers.
     * It should cover all the buffers with some different size combinations.
     * But, only needs a sample of codecs.
     */
    static class BufferCombinationsArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<BufferScenario> bufferScenarios = List.of(
                    new InMemoryBufferScenario(),
                    new LocalFileBufferScenario(),
                    new MultiPartBufferScenario()
            );
            final List<OutputScenario> outputScenarios = List.of(
                    new NdjsonOutputScenario(),
                    new ParquetOutputScenario()
            );
            final List<CompressionScenario> compressionScenarios = List.of(
                    new NoneCompressionScenario(),
                    new GZipCompressionScenario(),
                    new SnappyCompressionScenario()
            );
            final List<SizeCombination> sizeCombinations = List.of(
                    SizeCombination.EXACTLY_ONE,
                    SizeCombination.MEDIUM_SMALLER
            );

            return generateCombinedArguments(bufferScenarios, outputScenarios, compressionScenarios, sizeCombinations);
        }
    }

    /**
     * Some large file combinations are important to test, but large files can make the tests run a long time and
     * result in running out of memory. So this {@link ArgumentsProvider} attempts to get some particular large
     * file scenarios.
     * <p>
     * Testing larger files is particularly important for {@link MultiPartBufferScenario} because it has a minimum
     * upload size of 10MB. So if the size is below that, the test only covers one part. Also, Parquet can take a
     * larger size combination because it is an efficient format.
     */
    static class LargerBufferCombinationsArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(new ParquetOutputScenario(), new InMemoryBufferScenario(), new NoneCompressionScenario(), SizeCombination.LARGE), // 13.9 MB
                    arguments(new ParquetOutputScenario(), new InMemoryBufferScenario(), new GZipCompressionScenario(), SizeCombination.LARGE), // 8.9 MB
                    arguments(new ParquetOutputScenario(), new InMemoryBufferScenario(), new SnappyCompressionScenario(), SizeCombination.LARGE), // 12.6 MB
                    arguments(new NdjsonOutputScenario(), new MultiPartBufferScenario(), new NoneCompressionScenario(), SizeCombination.LARGE),  // 105.7 MB
                    arguments(new NdjsonOutputScenario(), new MultiPartBufferScenario(), new GZipCompressionScenario(), SizeCombination.LARGE), // 35.5 MB
                    arguments(new NdjsonOutputScenario(), new MultiPartBufferScenario(), new SnappyCompressionScenario(), SizeCombination.LARGE) // 70.6 MB
            );
        }
    }

    /**
     * Should test all codecs. It only varies some other conditions slightly.
     */
    static class CodecArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<BufferScenario> bufferScenarios = List.of(
                    new InMemoryBufferScenario(),
                    new MultiPartBufferScenario()
            );
            final List<OutputScenario> outputScenarios = List.of(
                    new NdjsonOutputScenario()
            );
            final List<CompressionScenario> compressionScenarios = List.of(
                    new NoneCompressionScenario(),
                    new GZipCompressionScenario()
            );
            final List<SizeCombination> sizeCombinations = List.of(
                    SizeCombination.MEDIUM_LARGER
            );

            return generateCombinedArguments(bufferScenarios, outputScenarios, compressionScenarios, sizeCombinations);
        }
    }

    static class OutputScenarioArguments implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments(new NdjsonOutputScenario()),
                    arguments(new JsonOutputScenario()));
        }
    }

    private static Stream<? extends Arguments> generateCombinedArguments(
            final List<BufferScenario> bufferScenarios,
            final List<OutputScenario> outputScenarios,
            final List<CompressionScenario> compressionScenarios,
            final List<SizeCombination> sizeCombinations) {
        return outputScenarios
                .stream()
                .flatMap(outputScenario -> bufferScenarios
                        .stream()
                        .filter(bufferScenario -> !outputScenario.getIncompatibleBufferTypes().contains(bufferScenario.getBufferType()))
                        .flatMap(bufferScenario -> compressionScenarios
                                .stream()
                                .flatMap(compressionScenario -> sizeCombinations
                                        .stream()
                                        .filter(sizeCombination -> sizeCombination.getTotalSize() <= bufferScenario.getMaximumNumberOfEvents())
                                        .map(sizeCombination -> arguments(outputScenario, bufferScenario, compressionScenario, sizeCombination))
                                )));
    }

    private static String reusableRandomString() {
        return reusableRandomStrings.get(RANDOM.nextInt(reusableRandomStrings.size()));
    }
}
