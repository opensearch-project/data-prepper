package org.opensearch.dataprepper.plugins.source.kinesis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.KinesisStreamConfig;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KinesisSourceTest {
    private final String PIPELINE_NAME = "kinesis-pipeline-test";
    private final String streamId = "stream-1";

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

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        kinesisSourceConfig = mock(KinesisSourceConfig.class);
        this.pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsAuthenticationConfig = mock(AwsAuthenticationConfig.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        kinesisService = mock(KinesisService.class);

        when(awsAuthenticationConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsExternalId()).thenReturn(UUID.randomUUID().toString());
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        when(kinesisSourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
    }

    public KinesisSource createObjectUnderTest() {
        return new KinesisSource(kinesisSourceConfig, pluginMetrics, pluginFactory, pipelineDescription, awsCredentialsSupplier, acknowledgementSetManager);
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
