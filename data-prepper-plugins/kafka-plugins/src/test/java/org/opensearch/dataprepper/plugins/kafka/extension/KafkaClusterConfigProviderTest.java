/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class KafkaClusterConfigProviderTest {
    @Mock
    private KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private KafkaClusterConfigProvider createObjectUnderTest() {
        return new KafkaClusterConfigProvider(kafkaClusterConfigSupplier);
    }

    @Test
    void supportedClass_returns_kafkaClusterConfigSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(KafkaClusterConfigSupplier.class));
    }

    @Test
    void provideInstance_returns_the_kafkaClusterConfigSupplier_from_the_constructor() {
        final KafkaClusterConfigProvider objectUnderTest = createObjectUnderTest();

        final Optional<KafkaClusterConfigSupplier> optionalKafkaClusterConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(optionalKafkaClusterConfigSupplier, notNullValue());
        assertThat(optionalKafkaClusterConfigSupplier.isPresent(), equalTo(true));
        assertThat(optionalKafkaClusterConfigSupplier.get(), equalTo(kafkaClusterConfigSupplier));

        final Optional<KafkaClusterConfigSupplier> anotherOptionalKafkaClusterConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalKafkaClusterConfigSupplier, notNullValue());
        assertThat(anotherOptionalKafkaClusterConfigSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalKafkaClusterConfigSupplier.get(), sameInstance(optionalKafkaClusterConfigSupplier.get()));
    }
}
