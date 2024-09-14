/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinlogClientFactoryTest {

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private RdsClient rdsClient;

    @Mock
    private DbMetadata dbMetadata;

    private BinlogClientFactory binlogClientFactory;
    private Random random;

    @BeforeEach
    void setUp() {
        binlogClientFactory = createBinlogClientFactory();
        random = new Random();
    }

    @Test
    void test_create() {
        final RdsSourceConfig.AuthenticationConfig authenticationConfig = mock(RdsSourceConfig.AuthenticationConfig.class);
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);

        binlogClientFactory.create();

        verify(dbMetadata).getHostName();
        verify(dbMetadata).getPort();
        verify(authenticationConfig).getUsername();
        verify(authenticationConfig).getPassword();
    }

    private BinlogClientFactory createBinlogClientFactory() {
        return new BinlogClientFactory(sourceConfig, rdsClient, dbMetadata);
    }
}