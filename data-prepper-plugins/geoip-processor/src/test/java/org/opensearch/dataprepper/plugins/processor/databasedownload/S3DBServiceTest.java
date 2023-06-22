/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.DownloadFailedException;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3DBServiceTest {

    private static final String S3_URI = "s3://mybucket10012023/GeoLite2/";
    private static final String PREFIX_DIR = "first_database";
    private static final String S3_REGION = "us-east-1";
    @Mock
    private GeoIPProcessorConfig geoIPProcessorConfig;

    @BeforeEach
    void setUp() {
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(geoIPProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(geoIPProcessorConfig.getAwsAuthenticationOptions().getAwsRegion()).thenReturn(Region.of(S3_REGION));
        AwsCredentialsProvider awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(geoIPProcessorConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration()).thenReturn(awsCredentialsProvider);
    }

    @Test
    void initiateDownloadTest_DownloadFailedException() throws NoSuchFieldException, IllegalAccessException {

        DatabasePathURLConfig databasePathURLConfig1 = new DatabasePathURLConfig();
        ReflectivelySetField.setField(DatabasePathURLConfig.class,
                databasePathURLConfig1, "url", S3_URI);

        List<DatabasePathURLConfig> config = new ArrayList<>();
        config.add(databasePathURLConfig1);

        S3DBService downloadThroughS3 = createObjectUnderTest();
        assertThrows(DownloadFailedException.class, () -> downloadThroughS3.initiateDownload(config));
    }

    private S3DBService createObjectUnderTest() {
        return new S3DBService(geoIPProcessorConfig, PREFIX_DIR);
    }
}