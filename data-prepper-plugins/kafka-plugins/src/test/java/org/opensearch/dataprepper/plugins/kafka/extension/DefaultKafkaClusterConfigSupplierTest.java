/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DefaultKafkaClusterConfigSupplierTest {
    @Mock
    private KafkaClusterConfig kafkaClusterConfig;

    private DefaultKafkaClusterConfigSupplier createObjectUnderTest() {
        return new DefaultKafkaClusterConfigSupplier(kafkaClusterConfig);
    }

    @Test
    void test_getters() {
        final List<String> bootstrapServers = List.of("localhost:9092");
        final AuthConfig authConfig = mock(AuthConfig.class);
        final AwsConfig awsConfig = mock(AwsConfig.class);
        final EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        when(kafkaClusterConfig.getBootStrapServers()).thenReturn(bootstrapServers);
        when(kafkaClusterConfig.getAuthConfig()).thenReturn(authConfig);
        when(kafkaClusterConfig.getAwsConfig()).thenReturn(awsConfig);
        when(kafkaClusterConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        DefaultKafkaClusterConfigSupplier defaultKafkaClusterConfigSupplier = createObjectUnderTest();
        assertThat(defaultKafkaClusterConfigSupplier.getBootStrapServers(), equalTo(bootstrapServers));
        assertThat(defaultKafkaClusterConfigSupplier.getAuthConfig(), equalTo(authConfig));
        assertThat(defaultKafkaClusterConfigSupplier.getAwsConfig(), equalTo(awsConfig));
        assertThat(defaultKafkaClusterConfigSupplier.getEncryptionConfig(), equalTo(encryptionConfig));
    }
}
