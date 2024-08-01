package org.opensearch.dataprepper.plugins.sink.personalize;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.PersonalizeSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.personalize.dataset.DatasetTypeOptions;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PersonalizeSinkTest {
    public static final int MAX_RETRIES = 10;
    public static final String REGION = "us-east-1";
    public static final String SINK_PLUGIN_NAME = "personalize";
    public static final String SINK_PIPELINE_NAME = "personalize-sink-pipeline";
    public static final String DATASET_ARN = "arn:aws:iam::123456789012:dataset/test";
    public static final String TRACKING_ID = "1233513241";
    private PersonalizeSinkConfiguration personalizeSinkConfig;
    private PersonalizeSink personalizeSink;
    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private SinkContext sinkContext;

    @BeforeEach
    void setup() {
        personalizeSinkConfig = mock(PersonalizeSinkConfiguration.class);
        sinkContext = mock(SinkContext.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        pluginSetting = mock(PluginSetting.class);
        pluginFactory = mock(PluginFactory.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        when(personalizeSinkConfig.getMaxRetries()).thenReturn(MAX_RETRIES);
        when(personalizeSinkConfig.getDatasetArn()).thenReturn(DATASET_ARN);
        when(personalizeSinkConfig.getDatasetType()).thenReturn(DatasetTypeOptions.USERS);
        when(personalizeSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(REGION));
        when(pluginSetting.getName()).thenReturn(SINK_PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(SINK_PIPELINE_NAME);
    }

    private PersonalizeSink createObjectUnderTest() {
        return new PersonalizeSink(pluginSetting, personalizeSinkConfig, pluginFactory, sinkContext, awsCredentialsSupplier);
    }

    @Test
    void test_personalize_sink_plugin_isReady_positive() {
        personalizeSink = createObjectUnderTest();
        Assertions.assertNotNull(personalizeSink);
        personalizeSink.doInitialize();
        assertTrue(personalizeSink.isReady(), "personalize sink is not initialized and not ready to work");
    }

    @Test
    void test_personalize_Sink_plugin_isReady_negative() {
        personalizeSink = createObjectUnderTest();
        Assertions.assertNotNull(personalizeSink);
        assertFalse(personalizeSink.isReady(), "personalize sink is initialized and ready to work");
    }

    @Test
    void test_doOutput_with_empty_records() {
        personalizeSink = createObjectUnderTest();
        Assertions.assertNotNull(personalizeSink);
        personalizeSink.doInitialize();
        Collection<Record<Event>> records = new ArrayList<>();
        personalizeSink.doOutput(records);
    }
}
