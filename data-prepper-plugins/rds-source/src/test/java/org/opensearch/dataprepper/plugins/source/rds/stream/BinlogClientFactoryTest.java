/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinlogClientFactoryTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private RdsClient rdsClient;

    @Mock
    private DbMetadata dbMetadata;

    private BinlogClientFactory binlogClientFactory;

    @Test
    void test_create() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
        when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

        binlogClientFactory = createObjectUnderTest();
        binlogClientFactory.create();

        verify(dbMetadata).getHostName();
        verify(dbMetadata).getPort();
    }

    private BinlogClientFactory createObjectUnderTest() {
        return new BinlogClientFactory(sourceConfig, rdsClient, dbMetadata);
    }
}