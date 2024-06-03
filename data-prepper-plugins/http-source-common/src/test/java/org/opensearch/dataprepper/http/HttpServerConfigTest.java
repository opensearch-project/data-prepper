/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.dataprepper.http.HttpServerConfig.S3_PREFIX;

public class HttpServerConfigTest {
    @Test
    void testDefault() {
        // Prepare
        final HttpServerConfig sourceConfig = new HttpServerConfig();

        // When/Then
        assertEquals(HttpServerConfig.DEFAULT_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(HttpServerConfig.DEFAULT_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(HttpServerConfig.DEFAULT_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(HttpServerConfig.DEFAULT_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
        assertEquals(HttpServerConfig.DEFAULT_USE_ACM_CERTIFICATE_FOR_SSL, sourceConfig.isUseAcmCertificateForSsl());
        assertEquals(HttpServerConfig.DEFAULT_ACM_CERTIFICATE_TIMEOUT_MILLIS, sourceConfig.getAcmCertificateTimeoutMillis());
        assertEquals((int)(HttpServerConfig.DEFAULT_REQUEST_TIMEOUT_MS * HttpServerConfig.BUFFER_TIMEOUT_FRACTION),
                     sourceConfig.getBufferTimeoutInMillis());
    }

    @Nested
    class SslValidationWithFile {
        @Test
        void isSslCertificateFileValidation_should_return_true_if_ssl_is_false() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", false);

            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
        }

        @Test
        void isSslCertificateFileValidation_should_return_false_if_ssl_is_true_and_sslCertificateFile_is_null() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);

            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(false));
        }

        @Test
        void isSslCertificateFileValidation_should_return_true_if_ssl_is_true_and_sslCertificateFile_is_a_valid_file() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
        }

        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_false() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", false);

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
        }

        @Test
        void isSslKeyFileValidation_should_return_false_if_ssl_is_true_and_sslKeyFile_is_null() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(false));
        }

        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_true_and_sslKeyFile_is_a_valid_file() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslKeyFile", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
        }

    }

    @Nested
    class SslValidationWithS3 {
        @Test
        void isSslCertAndKeyFileInS3_should_return_true_if_ssl_is_true_and_KeyFile_and_certFile_are_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", getS3FilePath());
            reflectivelySetField(objectUnderTest, "sslKeyFile", getS3FilePath());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
        }

        @Test
        void isSslCertAndKeyFileInS3_should_return_false_if_ssl_is_true_and_KeyFile_and_certFile_are_not_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", UUID.randomUUID().toString());
            reflectivelySetField(objectUnderTest, "sslKeyFile", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(false));
        }

        @Test
        void isAwsRegionValid_should_return_true_if_ssl_is_true_and_aws_region_is_null_without_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", UUID.randomUUID().toString());
            reflectivelySetField(objectUnderTest, "sslKeyFile", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(false));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
        }

        @Test
        void isAwsRegionValid_should_return_false_if_ssl_is_true_and_aws_region_is_null_with_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", getS3FilePath());
            reflectivelySetField(objectUnderTest, "sslKeyFile", getS3FilePath());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(false));
        }

        @Test
        void isAwsRegionValid_should_return_true_if_ssl_is_true_and_aws_region_is_not_null_with_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", getS3FilePath());
            reflectivelySetField(objectUnderTest, "sslKeyFile", getS3FilePath());
            reflectivelySetField(objectUnderTest, "awsRegion", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
        }

        @Test
        void isAwsRegionValid_should_return_false_if_ssl_is_true_and_aws_region_is_not_null_with_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", getS3FilePath());
            reflectivelySetField(objectUnderTest, "sslKeyFile", getS3FilePath());
            reflectivelySetField(objectUnderTest, "awsRegion", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
        }
    }

    @Nested
    class SslValidationWithAcm {
        @Test
        void isAwsRegionValid_should_return_false_if_ssl_is_true_and_aws_region_is_null_with_acm() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "useAcmCertificateForSsl", true);

            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(false));
        }

        @Test
        void isAwsRegionValid_should_return_true_if_ssl_is_true_and_aws_region_is_not_null_with_acm() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "useAcmCertificateForSsl", true);
            reflectivelySetField(objectUnderTest, "awsRegion", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
        }

        @Test
        void isAcmCertificateArnValid_should_return_false_if_ssl_is_true_and_acm_is_true_and_arn_is_null() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "useAcmCertificateForSsl", true);

            assertThat(objectUnderTest.isAcmCertificateArnValid(), equalTo(false));
        }

        @Test
        void isAcmCertificateArnValid_should_return_true_if_ssl_is_true_and_acm_is_true_and_arn_is_not_null() throws NoSuchFieldException, IllegalAccessException {
            final HttpServerConfig objectUnderTest = new HttpServerConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "useAcmCertificateForSsl", true);
            reflectivelySetField(objectUnderTest, "acmCertificateArn", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isAcmCertificateArnValid(), equalTo(true));
        }
    }

    private void reflectivelySetField(final HttpServerConfig HttpServerConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = HttpServerConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(HttpServerConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }

    private String getS3FilePath() {
        return S3_PREFIX.concat(UUID.randomUUID().toString());
    }
}
