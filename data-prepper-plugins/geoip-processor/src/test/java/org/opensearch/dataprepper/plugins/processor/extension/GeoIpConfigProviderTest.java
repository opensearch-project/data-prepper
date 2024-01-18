/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@ExtendWith(MockitoExtension.class)
class GeoIpConfigProviderTest {
    @Mock
    private GeoIpConfigSupplier geoIpConfigSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private GeoIpConfigProvider createObjectUnderTest() {
        return new GeoIpConfigProvider(geoIpConfigSupplier);
    }

    @Test
    void supportedClass_returns_geoIptConfigSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(GeoIpConfigSupplier.class));
    }

    @Test
    void provideInstance_returns_the_kafkaConnectConfigSupplier_from_the_constructor() {
        final GeoIpConfigProvider objectUnderTest = createObjectUnderTest();

        final Optional<GeoIpConfigSupplier> optionalKafkaConnectConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(optionalKafkaConnectConfigSupplier, notNullValue());
        assertThat(optionalKafkaConnectConfigSupplier.isPresent(), equalTo(true));
        assertThat(optionalKafkaConnectConfigSupplier.get(), equalTo(geoIpConfigSupplier));

        final Optional<GeoIpConfigSupplier> anotherOptionalKafkaConnectConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalKafkaConnectConfigSupplier, notNullValue());
        assertThat(anotherOptionalKafkaConnectConfigSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalKafkaConnectConfigSupplier.get(), sameInstance(optionalKafkaConnectConfigSupplier.get()));
    }

}