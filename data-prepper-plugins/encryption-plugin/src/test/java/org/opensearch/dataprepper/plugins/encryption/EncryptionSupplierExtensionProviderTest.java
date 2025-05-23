/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

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
class EncryptionSupplierExtensionProviderTest {
    @Mock
    private EncryptionSupplier encryptionSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private EncryptionSupplierExtensionProvider createObjectUnderTest() {
        return new EncryptionSupplierExtensionProvider(encryptionSupplier);
    }

    @Test
    void supportedClass_returns_encryptionSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(EncryptionSupplier.class));
    }

    @Test
    void provideInstance_returns_the_encryptionSupplier_from_the_constructor() {
        final EncryptionSupplierExtensionProvider objectUnderTest = createObjectUnderTest();

        final Optional<EncryptionSupplier> optionalEncryptionSupplier =
                objectUnderTest.provideInstance(context);
        assertThat(optionalEncryptionSupplier, notNullValue());
        assertThat(optionalEncryptionSupplier.isPresent(), equalTo(true));
        assertThat(optionalEncryptionSupplier.get(), equalTo(encryptionSupplier));

        final Optional<EncryptionSupplier> anotherOptionalEncryptionSupplier =
                objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalEncryptionSupplier, notNullValue());
        assertThat(anotherOptionalEncryptionSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalEncryptionSupplier.get(), sameInstance(optionalEncryptionSupplier.get()));
    }
}