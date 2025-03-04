/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.encryption.EncryptionHttpHandler;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class EncryptionHttpHandlerExtensionProviderTest {
    @Mock
    private EncryptionHttpHandler encryptionHttpHandler;
    @Mock
    private ExtensionProvider.Context context;

    private EncryptionHttpHandlerExtensionProvider createObjectUnderTest() {
        return new EncryptionHttpHandlerExtensionProvider(encryptionHttpHandler);
    }

    @Test
    void supportedClass_returns_encryptionHttpHandler() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(EncryptionHttpHandler.class));
    }

    @Test
    void provideInstance_returns_the_encryptionHttpHandler_from_the_constructor() {
        final EncryptionHttpHandlerExtensionProvider objectUnderTest = createObjectUnderTest();

        final Optional<EncryptionHttpHandler> optionalEncryptionHttpHandler =
                objectUnderTest.provideInstance(context);
        assertThat(optionalEncryptionHttpHandler, notNullValue());
        assertThat(optionalEncryptionHttpHandler.isPresent(), equalTo(true));
        assertThat(optionalEncryptionHttpHandler.get(), equalTo(encryptionHttpHandler));
    }
}