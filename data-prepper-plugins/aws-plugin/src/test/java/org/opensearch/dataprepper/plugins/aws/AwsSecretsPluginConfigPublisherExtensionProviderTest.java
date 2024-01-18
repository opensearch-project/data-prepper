/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class AwsSecretsPluginConfigPublisherExtensionProviderTest {
    @Mock
    private AwsSecretsPluginConfigPublisher awsSecretsPluginConfigPublisher;

    @Mock
    private ExtensionProvider.Context context;

    private AwsSecretsPluginConfigPublisherExtensionProvider createObjectUnderTest() {
        return new AwsSecretsPluginConfigPublisherExtensionProvider(awsSecretsPluginConfigPublisher);
    }

    @Test
    void supportedClass_returns_PluginConfigPublisher() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(PluginConfigPublisher.class));
    }

    @Test
    void provideInstance_returns_the_PluginConfigPublisher_from_the_constructor() {
        final AwsSecretsPluginConfigPublisherExtensionProvider objectUnderTest = createObjectUnderTest();

        final Optional<PluginConfigPublisher> optionalPluginConfigPublisher =
                objectUnderTest.provideInstance(context);
        assertThat(optionalPluginConfigPublisher, notNullValue());
        assertThat(optionalPluginConfigPublisher.isPresent(), equalTo(true));
        assertThat(optionalPluginConfigPublisher.get(), equalTo(awsSecretsPluginConfigPublisher));

        final Optional<PluginConfigPublisher> anotherOptionalPluginConfigPublisher =
                objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalPluginConfigPublisher, notNullValue());
        assertThat(anotherOptionalPluginConfigPublisher.isPresent(), equalTo(true));
        assertThat(anotherOptionalPluginConfigPublisher.get(), sameInstance(
                anotherOptionalPluginConfigPublisher.get()));
    }
}