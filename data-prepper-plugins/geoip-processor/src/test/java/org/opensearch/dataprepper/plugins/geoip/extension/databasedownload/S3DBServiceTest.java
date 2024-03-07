/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.AwsAuthenticationOptionsConfig;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3DBServiceTest {

    private static final String S3_URI = "s3://mybucket10012023/GeoLite2/";
    private static final String DATABASE_DIR = "blue_database";
    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;
    @Mock
    private AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig;

    @BeforeEach
    void setUp() {
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of("database-name", S3_URI));
    }

    @Test
    void initiateDownloadTest_DownloadFailedException() {
        S3DBService downloadThroughS3 = createObjectUnderTest();
        assertThrows(DownloadFailedException.class, () -> downloadThroughS3.initiateDownload());
    }

    private S3DBService createObjectUnderTest() {
        return new S3DBService(awsAuthenticationOptionsConfig, DATABASE_DIR, maxMindDatabaseConfig);
    }
}