package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.SecretsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class AwsSecretExtensionProviderTest {
    @Mock
    private AwsSecretsSupplier awsSecretsSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private AwsSecretExtensionProvider createObjectUnderTest() {
        return new AwsSecretExtensionProvider(awsSecretsSupplier);
    }

    @Test
    void supportedClass_returns_AwsSecretsSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(SecretsSupplier.class));
    }

    @Test
    void provideInstance_returns_the_AwsSecretsSupplier_from_the_constructor() {
        final AwsSecretExtensionProvider objectUnderTest = createObjectUnderTest();

        final Optional<SecretsSupplier> optionalSecretsSupplier = objectUnderTest.provideInstance(context);
        assertThat(optionalSecretsSupplier, notNullValue());
        assertThat(optionalSecretsSupplier.isPresent(), equalTo(true));
        assertThat(optionalSecretsSupplier.get(), equalTo(awsSecretsSupplier));

        final Optional<SecretsSupplier> anotherOptionalSecretsSupplier = objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalSecretsSupplier, notNullValue());
        assertThat(anotherOptionalSecretsSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalSecretsSupplier.get(), sameInstance(optionalSecretsSupplier.get()));
    }
}