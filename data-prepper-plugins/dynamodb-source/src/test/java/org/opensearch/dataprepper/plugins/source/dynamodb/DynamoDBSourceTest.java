/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import software.amazon.awssdk.regions.Region;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.ExportConfig;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

class DynamoDBSourceTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private DynamoDBSourceConfig dynamoDBSourceConfig;

    @Mock
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @Mock
    private TableConfig tableConfig;

    @Mock
    private ExportConfig exportConfig;

    private DynamoDBSource source;

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        dynamoDBSourceConfig = mock(DynamoDBSourceConfig.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        awsAuthenticationConfig = mock(AwsAuthenticationConfig.class);
        when(awsAuthenticationConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsExternalId()).thenReturn(UUID.randomUUID().toString());
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
        when(dynamoDBSourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        tableConfig = mock(TableConfig.class);
        exportConfig = mock(ExportConfig.class);
        when(tableConfig.getExportConfig()).thenReturn(exportConfig);
        when(dynamoDBSourceConfig.getTableConfigs()).thenReturn(List.of(tableConfig));
    }

    public DynamoDBSource createObjectUnderTest() {
        return new DynamoDBSource(pluginMetrics, dynamoDBSourceConfig, pluginFactory, awsCredentialsSupplier, acknowledgementSetManager);
    }

    @Test
    public void test_without_acknowledgements() {
        when(dynamoDBSourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);
        source = createObjectUnderTest();
        assertThat(source.areAcknowledgementsEnabled(), equalTo(false));
    }

    @Test
    public void test_with_acknowledgements() {
        when(dynamoDBSourceConfig.isAcknowledgmentsEnabled()).thenReturn(true);
        source = createObjectUnderTest();
        assertThat(source.areAcknowledgementsEnabled(), equalTo(true));

    }

}

