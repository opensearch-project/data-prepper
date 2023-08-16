/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
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
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3SinkIT {
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private SinkContext sinkContext;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ThresholdOptions thresholdOptions;
    @Mock
    private ObjectKeyOptions objectKeyOptions;
    private String s3region;
    private String bucketName;
    private S3Client s3Client;

    @TempDir
    private File s3FileLocation;
    private S3TransferManager transferManager;

    @BeforeEach
    void setUp() {
        when(pluginSetting.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        when(pluginSetting.getName()).thenReturn("s3");

        s3region = System.getProperty("tests.s3sink.region");

        final AwsCredentialsProvider awsCredentialsProvider = DefaultCredentialsProvider.create();

        final Region region = Region.of(s3region);
        s3Client = S3Client.builder().region(region).build();
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

        final S3AsyncClient s3AsyncClient = S3AsyncClient
                .builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(region)
                .build();

        transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }

    private S3Sink createObjectUnderTest() {
        return new S3Sink(pluginSetting, s3SinkConfig, pluginFactory, sinkContext, awsCredentialsSupplier);
    }

    @ParameterizedTest
    @ArgumentsSource(IntegrationTestArguments.class)
    void test(final OutputScenario outputScenario, final BufferTypeOptions bufferTypeOptions, final CompressionScenario compressionScenario, final int batchSize, final int numberOfBatches) throws IOException {

        final String pathPrefix = Instant.now().toString() + "-" + UUID.randomUUID();
        when(objectKeyOptions.getPathPrefix()).thenReturn(pathPrefix + "/");

        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any())).thenReturn(outputScenario.getCodec());
        when(s3SinkConfig.getBufferType()).thenReturn(bufferTypeOptions);
        when(s3SinkConfig.getCompression()).thenReturn(compressionScenario.getCompressionOption());
        when(thresholdOptions.getEventCount()).thenReturn(batchSize * numberOfBatches);

        final S3Sink objectUnderTest = createObjectUnderTest();

        final List<Map<String, Object>> allEventData = new ArrayList<>(batchSize * numberOfBatches);
        for (int batchNumber = 0; batchNumber < numberOfBatches; batchNumber++) {
            final int currentBatchNumber = batchNumber;
            final List<Record<Event>> events = IntStream.range(0, batchSize)
                    .mapToObj(sequence -> generateEventData(currentBatchNumber * sequence))
                    .peek(allEventData::add)
                    .map(this::generateTestEvent)
                    .map(Record::new)
                    .collect(Collectors.toList());

            objectUnderTest.doOutput(events);
        }

        assertThat(allEventData.size(), equalTo(batchSize * numberOfBatches));

        final ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(pathPrefix)
                .build());

        assertThat(listObjectsResponse.contents(), notNullValue());
        assertThat(listObjectsResponse.contents().size(), equalTo(1));

        final S3Object s3Object = listObjectsResponse.contents().get(0);

        final File target = new File(s3FileLocation, pathPrefix + ".original");

        final FileDownload fileDownload = transferManager.downloadFile(DownloadFileRequest.builder()
                .destination(target)
                .getObjectRequest(GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Object.key())
                        .build())
                .build());

        fileDownload.completionFuture().join();

        final File actualContentFile = new File(s3FileLocation, pathPrefix + ".content");
        IOUtils.copy(
                compressionScenario.decompressingInputStream(new FileInputStream(target)),
                new FileOutputStream(actualContentFile));

        outputScenario.validate(allEventData, actualContentFile);
    }

    private Event generateTestEvent(final Map<String, Object> eventData) {
        final EventMetadata defaultEventMetadata = DefaultEventMetadata.builder()
                .withEventType(EventType.LOG.toString())
                .build();
        final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventMetadata(defaultEventMetadata).build();
        event.setEventHandle(mock(EventHandle.class));
        return JacksonEvent.builder()
                .withData(eventData)
                .withEventMetadata(defaultEventMetadata)
                .build();
    }

    private static Map<String, Object> generateEventData(final int sequence) {
        final Map<String, Object> eventDataMap = new LinkedHashMap<>();
        eventDataMap.put("sequence", sequence);
        for (int i = 0; i < 2; i++) {
            eventDataMap.put("field" + i, UUID.randomUUID().toString());
            eventDataMap.put("float" + i, (float) i * 1.1);
        }
        for (int i = 0; i < 2; i++) {
            eventDataMap.put("list" + i,
                    List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        return eventDataMap;
    }

    static class IntegrationTestArguments implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            final List<BufferTypeOptions> bufferTypeOptions = Arrays.asList(BufferTypeOptions.values());
            final List<OutputScenario> outputScenarios = List.of(
                    new NdjsonOutputScenario());
            final List<CompressionScenario> compressionScenarios = List.of(
                    new NoneCompressionScenario(),
                    new GZipCompressionScenario()
            );
            final List<Integer> numberOfRecordsPerBatchList = List.of(1, 25, 500);
            final List<Integer> numberOfBatchesList = List.of(1, 25);

            return outputScenarios
                    .stream()
                    .flatMap(outputScenario -> bufferTypeOptions
                            .stream()
                            .flatMap(bufferTypeOption -> compressionScenarios
                                    .stream()
                                    .flatMap(compressionScenario -> numberOfRecordsPerBatchList
                                            .stream()
                                            .flatMap(batchRecordCount -> numberOfBatchesList
                                                    .stream()
                                                    .map(batchCount -> arguments(outputScenario, bufferTypeOption, compressionScenario, batchRecordCount, batchCount))
                                            ))));
        }
    }
}
