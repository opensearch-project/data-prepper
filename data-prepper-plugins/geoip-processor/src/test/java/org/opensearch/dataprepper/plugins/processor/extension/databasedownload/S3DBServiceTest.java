/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.exception.DownloadFailedException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class S3DBServiceTest {

    private static final String S3_URI = "s3://mybucket10012023/GeoLite2/";
    private static final String PREFIX_DIR = "first_database";
    private static final String S3_REGION = "us-east-1";
    @Mock
    private GeoIPProcessorConfig geoIPProcessorConfig;

    @BeforeEach
    void setUp() {

    }

    @Test
    void initiateDownloadTest_DownloadFailedException() {
        S3DBService downloadThroughS3 = createObjectUnderTest();
        assertThrows(DownloadFailedException.class, () -> downloadThroughS3.initiateDownload(List.of(S3_URI)));
    }

    private S3DBService createObjectUnderTest() {
        return new S3DBService(null, PREFIX_DIR);
    }
}