/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

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
public class KafkaConnectConfigProviderTest {
    @Mock
    private KafkaConnectConfigSupplier kafkaConnectConfigSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private KafkaConnectConfigProvider createObjectUnderTest() {
        return new KafkaConnectConfigProvider(kafkaConnectConfigSupplier);
    }

    @Test
    void supportedClass_returns_kafkaConnectConfigSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(KafkaConnectConfigSupplier.class));
    }

    @Test
    void provideInstance_returns_the_kafkaConnectConfigSupplier_from_the_constructor() {
        final KafkaConnectConfigProvider objectUnderTest = createObjectUnderTest();

        final Optional<KafkaConnectConfigSupplier> optionalKafkaConnectConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(optionalKafkaConnectConfigSupplier, notNullValue());
        assertThat(optionalKafkaConnectConfigSupplier.isPresent(), equalTo(true));
        assertThat(optionalKafkaConnectConfigSupplier.get(), equalTo(kafkaConnectConfigSupplier));

        final Optional<KafkaConnectConfigSupplier> anotherOptionalKafkaConnectConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalKafkaConnectConfigSupplier, notNullValue());
        assertThat(anotherOptionalKafkaConnectConfigSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalKafkaConnectConfigSupplier.get(), sameInstance(optionalKafkaConnectConfigSupplier.get()));
    }
}
