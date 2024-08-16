package org.opensearch.dataprepper.plugins.kinesis.extension;

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
public class KinesisLeaseConfigProviderTest {
    @Mock
    private KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;

    @Mock
    private ExtensionProvider.Context context;

    private KinesisLeaseConfigProvider createObjectUnderTest() {
        return new KinesisLeaseConfigProvider(kinesisLeaseConfigSupplier);
    }

    @Test
    void supportedClassReturnsKinesisSourceConfigSupplier() {
        assertThat(createObjectUnderTest().supportedClass(), equalTo(KinesisLeaseConfigSupplier.class));
    }

    @Test
    void provideInstanceReturnsKinesisSourceConfigSupplierFromConstructor() {
        final KinesisLeaseConfigProvider objectUnderTest = createObjectUnderTest();

        final Optional<KinesisLeaseConfigSupplier> optionalKinesisSourceConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(optionalKinesisSourceConfigSupplier, notNullValue());
        assertThat(optionalKinesisSourceConfigSupplier.isPresent(), equalTo(true));
        assertThat(optionalKinesisSourceConfigSupplier.get(), equalTo(kinesisLeaseConfigSupplier));

        final Optional<KinesisLeaseConfigSupplier> anotherOptionalKinesisSourceConfigSupplier = objectUnderTest.provideInstance(context);
        assertThat(anotherOptionalKinesisSourceConfigSupplier, notNullValue());
        assertThat(anotherOptionalKinesisSourceConfigSupplier.isPresent(), equalTo(true));
        assertThat(anotherOptionalKinesisSourceConfigSupplier.get(), sameInstance(optionalKinesisSourceConfigSupplier.get()));
    }
}
