/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.dataprepper.http.common.HttpServerConfig.S3_PREFIX;

public class HttpServerConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "http";

    private static Stream<Arguments> provideCompressionOption() {
        return Stream.of(Arguments.of(CompressionOption.GZIP));
    }

    @Test
    void testDefault() {
        // Prepare
        final HttpServerConfig sourceConfig = new HttpServerConfig();

        // When/Then
        assertEquals(HttpServerConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(HttpServerConfig.DEFAULT_LOG_INGEST_URI, sourceConfig.getPath());
        assertEquals(HttpServerConfig.DEFAULT_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(HttpServerConfig.DEFAULT_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(HttpServerConfig.DEFAULT_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(HttpServerConfig.DEFAULT_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
        assertEquals(HttpServerConfig.DEFAULT_USE_ACM_CERTIFICATE_FOR_SSL, sourceConfig.isUseAcmCertificateForSsl());
        assertEquals(HttpServerConfig.DEFAULT_ACM_CERTIFICATE_TIMEOUT_MILLIS, sourceConfig.getAcmCertificateTimeoutMillis());
        assertEquals((int)(HttpServerConfig.DEFAULT_REQUEST_TIMEOUT_MS * HttpServerConfig.BUFFER_TIMEOUT_FRACTION),
                     sourceConfig.getBufferTimeoutInMillis());
        assertEquals(CompressionOption.NONE, sourceConfig.getCompression());
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

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(HttpServerConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final HttpServerConfig HttpServerConfig = OBJECT_MAPPER.convertValue(
                pluginSetting.getSettings(), HttpServerConfig.class);

        // When/Then
        assertEquals(compressionOption, HttpServerConfig.getCompression());
    }

    @Test
    void getPath_should_return_correct_path() throws NoSuchFieldException, IllegalAccessException {
        final HttpServerConfig objectUnderTest = new HttpServerConfig();

        reflectivelySetField(objectUnderTest, "path", "/my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(true));
        assertThat(objectUnderTest.getPath(), equalTo("/my/custom/path"));
    }

    @Test
    void isPathValid_should_return_false_for_invalid_path() throws NoSuchFieldException, IllegalAccessException {
        final HttpServerConfig objectUnderTest = new HttpServerConfig();

        reflectivelySetField(objectUnderTest, "path", "my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(false));
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
