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
public class KinesisLeaseConfigSupplierTest {
    private static final String LEASE_COORDINATION_TABLE = "lease-table";
    @Mock
    KinesisLeaseConfig kinesisLeaseConfig;

    @Mock
    KinesisLeaseCoordinationTableConfig kinesisLeaseCoordinationTableConfig;

    private KinesisLeaseConfigSupplier createObjectUnderTest() {
        return new KinesisLeaseConfigSupplier(kinesisLeaseConfig);
    }

    @Test
    void testGetters() {
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(kinesisLeaseCoordinationTableConfig);
        when(kinesisLeaseCoordinationTableConfig.getTableName()).thenReturn(LEASE_COORDINATION_TABLE);
        when(kinesisLeaseCoordinationTableConfig.getRegion()).thenReturn("us-east-1");
        KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier = createObjectUnderTest();
        assertThat(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().get().getLeaseCoordinationTable().getTableName(), equalTo(LEASE_COORDINATION_TABLE));
        assertThat(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().get().getLeaseCoordinationTable().getRegion(), equalTo("us-east-1"));
    }

    @Test
    void testGettersWithNullTableConfig() {
        when(kinesisLeaseConfig.getLeaseCoordinationTable()).thenReturn(null);
        KinesisLeaseConfigSupplier defaultKinesisLeaseConfigSupplier = createObjectUnderTest();
        assertThat(defaultKinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig().get().getLeaseCoordinationTable(), equalTo(null));

    }

    @Test
    void testGettersWithNullConfig() {
        KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier = new KinesisLeaseConfigSupplier(null);
        assertThat(kinesisLeaseConfigSupplier.getKinesisExtensionLeaseConfig(), equalTo(Optional.empty()));
    }
}
