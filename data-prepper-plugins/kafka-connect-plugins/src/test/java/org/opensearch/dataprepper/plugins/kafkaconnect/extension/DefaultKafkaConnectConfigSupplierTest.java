/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class DefaultKafkaConnectConfigSupplierTest {
    @Mock
    private KafkaConnectConfig kafkaConnectConfig;

    private DefaultKafkaConnectConfigSupplier createObjectUnderTest() {
        return new DefaultKafkaConnectConfigSupplier(kafkaConnectConfig);
    }

    @Test
    void test_get_config() {
        assertThat(createObjectUnderTest().getConfig(), equalTo(kafkaConnectConfig));
    }
}
