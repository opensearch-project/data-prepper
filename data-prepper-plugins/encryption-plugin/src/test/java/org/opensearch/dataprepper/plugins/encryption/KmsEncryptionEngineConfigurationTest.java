/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.services.kms.KmsClient;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KmsEncryptionEngineConfigurationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    @ParameterizedTest
    @ValueSource(strings = {
            "/test-kms-encryption-engine-config-invalid-sts-1.yaml",
            "/test-kms-encryption-engine-config-invalid-sts-2.yaml"
    })
    void testInvalidStsRoleArn(final String testFileName) throws IOException {
        final InputStream inputStream = KmsEncryptionEngineConfigurationTest.class.getResourceAsStream(testFileName);
        final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration = OBJECT_MAPPER.readValue(
                inputStream, KmsEncryptionEngineConfiguration.class);
        assertThat(kmsEncryptionEngineConfiguration.validateStsRoleArn(), is(false));
    }

    @Test
    void testMalformatStsRoleArn() throws IOException {
        final InputStream inputStream = KmsEncryptionEngineConfigurationTest.class.getResourceAsStream(
                "/test-kms-encryption-engine-config-malformat-sts.yaml");
        final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration = OBJECT_MAPPER.readValue(
                inputStream, KmsEncryptionEngineConfiguration.class);
        assertThrows(IllegalArgumentException.class, () -> kmsEncryptionEngineConfiguration.validateStsRoleArn());
    }

    @Test
    void testCreateKmsClient() throws IOException {
        final InputStream inputStream = KmsEncryptionEngineConfigurationTest.class.getResourceAsStream(
                "/test-valid-kms-encryption-engine-config.yaml");
        final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration = OBJECT_MAPPER.readValue(
                inputStream, KmsEncryptionEngineConfiguration.class);
        assertThat(kmsEncryptionEngineConfiguration.createKmsClient(), instanceOf(KmsClient.class));
    }

    @Test
    void testStsHeaderOverridesPassedThroughToCredentialsOptions() throws IOException {
        final InputStream inputStream = KmsEncryptionEngineConfigurationTest.class.getResourceAsStream(
                "/test-kms-encryption-engine-config-with-header-overrides.yaml");
        final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration = OBJECT_MAPPER.readValue(
                inputStream, KmsEncryptionEngineConfiguration.class);
        final AwsCredentialsOptions credentialsOptions = kmsEncryptionEngineConfiguration.toCredentialsOptions();
        assertThat(credentialsOptions, is(notNullValue()));
        assertThat(credentialsOptions.getStsHeaderOverrides(), is(notNullValue()));
        assertThat(credentialsOptions.getStsHeaderOverrides(),
                hasEntry("x-amz-source-arn", "arn:aws:osis:us-east-1:123456789012:pipeline/test-pipeline"));
        assertThat(credentialsOptions.getStsHeaderOverrides(),
                hasEntry("x-amz-source-account", "123456789012"));
    }

    @Test
    void testToCredentialsOptionsWithoutHeaderOverrides() throws IOException {
        final InputStream inputStream = KmsEncryptionEngineConfigurationTest.class.getResourceAsStream(
                "/test-valid-kms-encryption-engine-config.yaml");
        final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration = OBJECT_MAPPER.readValue(
                inputStream, KmsEncryptionEngineConfiguration.class);
        final AwsCredentialsOptions credentialsOptions = kmsEncryptionEngineConfiguration.toCredentialsOptions();
        assertThat(credentialsOptions, is(notNullValue()));
        assertThat(credentialsOptions.getStsHeaderOverrides().isEmpty(), is(true));
    }
}