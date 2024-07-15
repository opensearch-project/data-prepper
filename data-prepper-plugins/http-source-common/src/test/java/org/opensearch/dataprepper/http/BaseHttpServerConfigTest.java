/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.dataprepper.http.BaseHttpServerConfig.S3_PREFIX;

public class BaseHttpServerConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "http";
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";

    private static Stream<Arguments> provideCompressionOption() {
        return Stream.of(Arguments.of(CompressionOption.GZIP));
    }

    @Test
    void testDefault() {
        // Prepare
        final HttpServerConfig sourceConfig = new BaseHttpServerConfig();

        // When/Then
        assertEquals(BaseHttpServerConfig.DEFAULT_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(BaseHttpServerConfig.DEFAULT_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(BaseHttpServerConfig.DEFAULT_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(BaseHttpServerConfig.DEFAULT_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
        assertEquals(BaseHttpServerConfig.DEFAULT_USE_ACM_CERTIFICATE_FOR_SSL, sourceConfig.isUseAcmCertificateForSsl());
        assertEquals(BaseHttpServerConfig.DEFAULT_ACM_CERTIFICATE_TIMEOUT_MILLIS, sourceConfig.getAcmCertificateTimeoutMillis());
        assertEquals((int)(BaseHttpServerConfig.DEFAULT_REQUEST_TIMEOUT_MS * BaseHttpServerConfig.BUFFER_TIMEOUT_FRACTION),
                     sourceConfig.getBufferTimeoutInMillis());
    }

    @Test
    void getPath_should_return_correct_path() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "path", "/my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(true));
        assertThat(objectUnderTest.getPath(), equalTo("/my/custom/path"));
    }

    @Test
    void isPathValid_should_return_false_for_invalid_path() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "path", "my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(false));
    }

    @Test
    void testValidPort() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "port", 2021);

        assertThat(objectUnderTest.getPort(), equalTo(2021));
    }

    @Test
    void testValidAWSRegion() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "awsRegion", "us-east-1");

        assertThat(objectUnderTest.getAwsRegion(), equalTo("us-east-1"));
    }

    @Test
    void testMaxRequestLength() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "maxRequestLength", ByteCount.ofBytes(4));

        assertThat(objectUnderTest.getMaxRequestLength(), equalTo(ByteCount.ofBytes(4)));
    }

    @Test
    void testHealthCheckService() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "healthCheckService", true);

        assertEquals(objectUnderTest.hasHealthCheckService(), true);
    }

    @Test
    void testUnauthenticatedHealthCheck() throws NoSuchFieldException, IllegalAccessException {
        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "unauthenticatedHealthCheck", true);

        assertThat(objectUnderTest.isUnauthenticatedHealthCheck(), equalTo(true));
    }

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(BaseHttpServerConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final BaseHttpServerConfig httpSourceConfig = OBJECT_MAPPER.convertValue(
                pluginSetting.getSettings(), BaseHttpServerConfig.class);

        // When/Then
        assertEquals(compressionOption, httpSourceConfig.getCompression());
    }

    @Test
    void testAuthentication() throws NoSuchFieldException, IllegalAccessException {
        PluginModel authentication = new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                ));

        final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

        ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "authentication", authentication);

        assertThat(objectUnderTest.getAuthentication(), equalTo(authentication));
    }

    @Nested
    class SslValidationWithFile {
        @Test
        void isSslCertificateFileValidation_should_return_true_if_ssl_is_false() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", false);

            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSsl(), equalTo(false));
        }

        @Test
        void isSslCertificateFileValidation_should_return_false_if_ssl_is_true_and_sslCertificateFile_is_null() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);

            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(false));
            assertThat(objectUnderTest.isSsl(), equalTo(true));
        }

        @Test
        void isSslCertificateFileValidation_should_return_true_if_ssl_is_true_and_sslCertificateFile_is_a_valid_file() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);

            final String sslCertificateFile = UUID.randomUUID().toString();
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", sslCertificateFile);

            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.getSslCertificateFile(), equalTo(sslCertificateFile));
        }

        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_false() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", false);

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
        }

        @Test
        void isSslKeyFileValidation_should_return_false_if_ssl_is_true_and_sslKeyFile_is_null() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(false));
        }

        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_true_and_sslKeyFile_is_a_valid_file() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);

            final String sslKeyFile = UUID.randomUUID().toString();
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", sslKeyFile);

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.getSslKeyFile(), equalTo(sslKeyFile));
        }

    }

    @Nested
    class SslValidationWithS3 {
        @Test
        void isSslCertAndKeyFileInS3_should_return_true_if_ssl_is_true_and_KeyFile_and_certFile_are_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", getS3FilePath());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", getS3FilePath());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
        }

        @Test
        void isSslCertAndKeyFileInS3_should_return_false_if_ssl_is_true_and_KeyFile_and_certFile_are_not_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", UUID.randomUUID().toString());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(false));
        }

        @Test
        void isAwsRegionValid_should_return_true_if_ssl_is_true_and_aws_region_is_null_without_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", UUID.randomUUID().toString());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(false));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
        }

        @Test
        void isAwsRegionValid_should_return_false_if_ssl_is_true_and_aws_region_is_null_with_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", getS3FilePath());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", getS3FilePath());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(false));
        }

        @Test
        void isAwsRegionValid_should_return_true_if_ssl_is_true_and_aws_region_is_not_null_with_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", getS3FilePath());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", getS3FilePath());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "awsRegion", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isSslKeyFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertificateFileValid(), equalTo(true));
            assertThat(objectUnderTest.isSslCertAndKeyFileInS3(), equalTo(true));
            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
        }

        @Test
        void isAwsRegionValid_should_return_false_if_ssl_is_true_and_aws_region_is_not_null_with_s3_paths() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslCertificateFile", getS3FilePath());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "sslKeyFile", getS3FilePath());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "awsRegion", UUID.randomUUID().toString());

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
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "useAcmCertificateForSsl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "acmCertificateArn", "acm-certificate-arn");

            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(false));
            assertThat(objectUnderTest.getAcmCertificateArn(), equalTo("acm-certificate-arn"));
        }

        @Test
        void isAwsRegionValid_should_return_true_if_ssl_is_true_and_aws_region_is_not_null_with_acm() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "useAcmCertificateForSsl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "awsRegion", UUID.randomUUID().toString());
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "acmCertificateArn", "acm-certificate-arn");

            assertThat(objectUnderTest.isAwsRegionValid(), equalTo(true));
            assertThat(objectUnderTest.getAcmCertificateArn(), equalTo("acm-certificate-arn"));
        }

        @Test
        void isAcmCertificateArnValid_should_return_false_if_ssl_is_true_and_acm_is_true_and_arn_is_null() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "useAcmCertificateForSsl", true);

            assertThat(objectUnderTest.isAcmCertificateArnValid(), equalTo(false));
        }

        @Test
        void isAcmCertificateArnValid_should_return_true_if_ssl_is_false_and_acm_is_true_and_arn_is_null() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", false);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "useAcmCertificateForSsl", true);

            assertThat(objectUnderTest.isAcmCertificateArnValid(), equalTo(true));
        }

        @Test
        void isAcmCertificateArnValid_should_return_true_if_ssl_is_true_and_acm_is_true_and_arn_is_not_null() throws NoSuchFieldException, IllegalAccessException {
            final BaseHttpServerConfig objectUnderTest = new BaseHttpServerConfig();

            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "ssl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "useAcmCertificateForSsl", true);
            ReflectivelySetField.setField(BaseHttpServerConfig.class, objectUnderTest, "acmCertificateArn", UUID.randomUUID().toString());

            assertThat(objectUnderTest.isAcmCertificateArnValid(), equalTo(true));
        }
    }

    private String getS3FilePath() {
        return S3_PREFIX.concat(UUID.randomUUID().toString());
    }
}
