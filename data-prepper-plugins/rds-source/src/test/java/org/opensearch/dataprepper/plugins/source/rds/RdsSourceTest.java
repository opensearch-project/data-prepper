/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class RdsSourceTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private EventFactory eventFactory;

    @Mock
    AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @BeforeEach
    void setUp() {
        when(sourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
    }

    @Test
    void test_when_buffer_is_null_then_start_throws_exception() {
        RdsSource objectUnderTest = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> objectUnderTest.start(null));
    }

    private RdsSource createObjectUnderTest() {
        return new RdsSource(pluginMetrics, sourceConfig, eventFactory, awsCredentialsSupplier);
    }
}