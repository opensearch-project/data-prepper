/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class HTTPSourceConfigTest {
    @Test
    public void testDefault() {
        // Prepare
        final HTTPSourceConfig sourceConfig = new HTTPSourceConfig();

        // When/Then
        assertEquals(HTTPSourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(HTTPSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(HTTPSourceConfig.DEFAULT_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(HTTPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(HTTPSourceConfig.DEFAULT_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
    }

    @Nested
    class Validation {
        @TempDir
        File temporaryDirectory;
        private File file;

        @BeforeEach
        void setUp() throws IOException {
            file = new File(temporaryDirectory, UUID.randomUUID().toString());
            file.createNewFile();
        }

        @Test
        void isSslCertificateFileValidation_should_return_true_if_ssl_is_false() throws NoSuchFieldException, IllegalAccessException {
            final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

            reflectivelySetField(objectUnderTest, "ssl", false);

            assertThat(objectUnderTest.isSslCertificateFileValidation(), equalTo(true));
        }

        @Test
        void isSslCertificateFileValidation_should_return_false_if_ssl_is_true_and_sslCertificateFile_is_null() throws NoSuchFieldException, IllegalAccessException {
            final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);

            assertThat(objectUnderTest.isSslCertificateFileValidation(), equalTo(false));
        }

        @Test
        void isSslCertificateFileValidation_should_return_false_if_ssl_is_true_and_sslCertificateFile_is_a_valid_file() throws NoSuchFieldException, IllegalAccessException {
            final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslCertificateFile", file.getAbsolutePath());

            assertThat(objectUnderTest.isSslCertificateFileValidation(), equalTo(true));
        }

        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_false() throws NoSuchFieldException, IllegalAccessException {
            final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

            reflectivelySetField(objectUnderTest, "ssl", false);

            assertThat(objectUnderTest.isSslKeyFileValidation(), equalTo(true));
        }

        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_false_and_sslKeyFile_is_null() throws NoSuchFieldException, IllegalAccessException {
            final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);

            assertThat(objectUnderTest.isSslKeyFileValidation(), equalTo(false));
        }
        @Test
        void isSslKeyFileValidation_should_return_true_if_ssl_is_false_and_sslKeyFile_is_a_valid_file() throws NoSuchFieldException, IllegalAccessException {
            final HTTPSourceConfig objectUnderTest = new HTTPSourceConfig();

            reflectivelySetField(objectUnderTest, "ssl", true);
            reflectivelySetField(objectUnderTest, "sslKeyFile", file.getAbsolutePath());

            assertThat(objectUnderTest.isSslKeyFileValidation(), equalTo(true));
        }

        private void reflectivelySetField(final HTTPSourceConfig httpSourceConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
            final Field field = HTTPSourceConfig.class.getDeclaredField(fieldName);
            try {
                field.setAccessible(true);
                field.set(httpSourceConfig, value);
            } finally {
                field.setAccessible(false);
            }
        }
    }
}