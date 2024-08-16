package org.opensearch.dataprepper.plugins.kinesis.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DefaultKinesisLeaseConfigSupplierTest {
    private static final String LEASE_COORDINATION_TABLE = "lease-table";
    @Mock
    KinesisLeaseConfig kinesisLeaseConfig;

    private DefaultKinesisLeaseConfigSupplier createObjectUnderTest() {
        return new DefaultKinesisLeaseConfigSupplier(kinesisLeaseConfig);
    }

    @Test
    void testGetters() {
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(LEASE_COORDINATION_TABLE);
        KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier = createObjectUnderTest();
        assertThat(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().get().getLeaseCoordinationTable(), equalTo(LEASE_COORDINATION_TABLE));
    }

    @Test
    void testGettersWithNullTableConfig() {
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(null);
        DefaultKinesisLeaseConfigSupplier defaultKinesisLeaseConfigSupplier = createObjectUnderTest();
        assertThat(defaultKinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().get().getLeaseCoordinationTable(), equalTo(null));

    }

    @Test
    void testGettersWithNullConfig() {
        KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier = new DefaultKinesisLeaseConfigSupplier(null);
        assertThat(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig(), equalTo(Optional.empty()));
    }
}
