/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class AwsExtensionProviderTest {
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private AwsExtensionProvider createObjectUnderTest() {
        return new AwsExtensionProvider(awsCredentialsSupplier);
    }

    @Test
    void supportedClass_returns_AwsCredentialsSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(AwsCredentialsSupplier.class));
    }

    @Test
    void provideInstance_returns_the_AwsCredentialsSupplier_from_the_constructor() {
        final AwsExtensionProvider objectUnderTest = createObjectUnderTest();

        final Optional<AwsCredentialsSupplier> optionalCredentialsSupplier = objectUnderTest.provideInstance(context);
        assertThat(optionalCredentialsSupplier, notNullValue());
        assertThat(optionalCredentialsSupplier.isPresent(), equalTo(true));
        assertThat(optionalCredentialsSupplier.get(), equalTo(awsCredentialsSupplier));

        final Optional<AwsCredentialsSupplier> anotherOptionalCredentialsSupplier = objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalCredentialsSupplier, notNullValue());
        assertThat(anotherOptionalCredentialsSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalCredentialsSupplier.get(), sameInstance(optionalCredentialsSupplier.get()));
    }
}