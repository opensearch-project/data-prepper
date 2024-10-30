/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfigSupplier;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseCoordinationTableConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KinesisSourceTest {
    private final String PIPELINE_NAME = "kinesis-pipeline-test";
    private final String streamId = "stream-1";
    private static final String codec_plugin_name = "json";
    private String pipelineIdentifier;
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private AwsAuthenticationConfig awsAuthenticationConfig;

    private KinesisSource source;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock KinesisService kinesisService;

    @Mock
    KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;

    @Mock
    KinesisLeaseConfig kinesisLeaseConfig;

    @Mock
    KinesisLeaseCoordinationTableConfig kinesisLeaseCoordinationTableConfig;

    @BeforeEach
    void setup() {
        pipelineIdentifier = UUID.randomUUID().toString();
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        kinesisSourceConfig = mock(KinesisSourceConfig.class);
        this.pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsAuthenticationConfig = mock(AwsAuthenticationConfig.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        kinesisService = mock(KinesisService.class);

        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(codec_plugin_name);
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);

        pluginFactory = mock(PluginFactory.class);
        InputCodec codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);

        kinesisLeaseConfigSupplier = mock(KinesisLeaseConfigSupplier.class);
        kinesisLeaseConfig = mock(KinesisLeaseConfig.class);
        when(kinesisLeaseConfig.getPipelineIdentifier()).thenReturn(pipelineIdentifier);
        kinesisLeaseCoordinationTableConfig = mock(KinesisLeaseCoordinationTableConfig.class);
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(kinesisLeaseCoordinationTableConfig);
        when(kinesisLeaseCoordinationTableConfig.getTableName()).thenReturn("table-name");
        when(kinesisLeaseCoordinationTableConfig.getRegion()).thenReturn("us-east-1");
        when(kinesisLeaseCoordinationTableConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig()).thenReturn(Optional.ofNullable(kinesisLeaseConfig));
        when(awsAuthenticationConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsExternalId()).thenReturn(UUID.randomUUID().toString());
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        AwsCredentialsProvider defaultCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(AwsCredentialsOptions.defaultOptions())).thenReturn(defaultCredentialsProvider);
        when(awsAuthenticationConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        when(kinesisSourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
    }

    public KinesisSource createObjectUnderTest() {
        return new KinesisSource(kinesisSourceConfig, pluginMetrics, pluginFactory, pipelineDescription, awsCredentialsSupplier, acknowledgementSetManager, kinesisLeaseConfigSupplier);
    }

    @Test
    public void testSourceWithoutAcknowledgements() {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(false);
        source = createObjectUnderTest();
        assertThat(source.areAcknowledgementsEnabled(), equalTo(false));
    }

    @Test
    public void testSourceWithAcknowledgements() {
        when(kinesisSourceConfig.isAcknowledgments()).thenReturn(true);
        source = createObjectUnderTest();
        assertThat(source.areAcknowledgementsEnabled(), equalTo(true));
    }

    @Test
    public void testSourceStart() {

        source = createObjectUnderTest();

        Buffer<Record<Event>> buffer = mock(Buffer.class);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(100);
        KinesisStreamConfig kinesisStreamConfig = mock(KinesisStreamConfig.class);
        when(kinesisStreamConfig.getName()).thenReturn(streamId);
        when(kinesisSourceConfig.getStreams()).thenReturn(List.of(kinesisStreamConfig));
        source.setKinesisService(kinesisService);

        source.start(buffer);

        verify(kinesisService, times(1)).start(any(Buffer.class));

    }

    @Test
    public void testSourceStartBufferNull() {

        source = createObjectUnderTest();

        assertThrows(IllegalStateException.class, () -> source.start(null));

        verify(kinesisService, times(0)).start(any(Buffer.class));

    }

    @Test
    public void testSourceStop() {

        source = createObjectUnderTest();

        source.setKinesisService(kinesisService);

        source.stop();

        verify(kinesisService, times(1)).shutDown();

    }

}
