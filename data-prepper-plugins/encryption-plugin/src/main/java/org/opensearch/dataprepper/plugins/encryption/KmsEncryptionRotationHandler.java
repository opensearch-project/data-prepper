/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyWithoutPlaintextRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyWithoutPlaintextResponse;

import java.util.Base64;

public class KmsEncryptionRotationHandler implements EncryptionRotationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KmsEncryptionRotationHandler.class);

    static final String ENCRYPTION_ROTATION_SUCCESS = "encryptionRotationSuccess";
    static final String ENCRYPTION_ROTATION_FAILURE = "encryptionRotationFailure";
    static final String ENCRYPTION_ROTATION_DURATION = "encryptionRotationDuration";
    static final String ENCRYPTION_ID_TAG = "encryptionId";

    private final String encryptionId;
    private final KmsClient kmsClient;
    private final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    private final EncryptedDataKeyWriter encryptedDataKeyWriter;
    private final PluginMetrics pluginMetrics;
    private final Counter encryptionRotationSuccessCounter;
    private final Counter encryptionRotationFailureCounter;
    private final Timer encryptionRotationTimer;

    public KmsEncryptionRotationHandler(final String encryptionId,
                                        final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration,
                                        final EncryptedDataKeyWriter encryptedDataKeyWriter,
                                        final PluginMetrics pluginMetrics) {
        this.encryptionId = encryptionId;
        this.kmsEncryptionEngineConfiguration = kmsEncryptionEngineConfiguration;
        kmsClient = kmsEncryptionEngineConfiguration.createKmsClient();
        this.encryptedDataKeyWriter = encryptedDataKeyWriter;
        this.pluginMetrics = pluginMetrics;
        this.encryptionRotationSuccessCounter = pluginMetrics.counterWithTags(
                ENCRYPTION_ROTATION_SUCCESS, ENCRYPTION_ID_TAG, encryptionId);
        this.encryptionRotationFailureCounter = pluginMetrics.counterWithTags(
                ENCRYPTION_ROTATION_FAILURE, ENCRYPTION_ID_TAG, encryptionId);
        this.encryptionRotationTimer = pluginMetrics.timerWithTags(
                ENCRYPTION_ROTATION_DURATION, ENCRYPTION_ID_TAG, encryptionId);
    }

    @Override
    public String getEncryptionId() {
        return encryptionId;
    }

    @Override
    public void handleRotation() {
        encryptionRotationTimer.record(() -> {
            try {
                final String encodedEncryptedDataKey = generateNewEncodedEncryptedDataKey();
                encryptedDataKeyWriter.writeEncryptedDataKey(encodedEncryptedDataKey);
                encryptionRotationSuccessCounter.increment();
            } catch (Exception e) {
                LOG.error("Failed to rotate encrypted data key in encryption: {}.", encryptionId, e);
                encryptionRotationFailureCounter.increment();
            }
        });
    }

    private String generateNewEncodedEncryptedDataKey() {
        final GenerateDataKeyWithoutPlaintextRequest generateDataKeyWithoutPlaintextRequest =
                GenerateDataKeyWithoutPlaintextRequest.builder()
                        .keyId(kmsEncryptionEngineConfiguration.getKeyId())
                        .encryptionContext(kmsEncryptionEngineConfiguration.getEncryptionContext())
                        .keySpec(DataKeySpec.AES_256)
                        .build();
        final GenerateDataKeyWithoutPlaintextResponse generateDataKeyWithoutPlaintextResponse =
                kmsClient.generateDataKeyWithoutPlaintext(generateDataKeyWithoutPlaintextRequest);
        final byte[] encryptedDataKey = generateDataKeyWithoutPlaintextResponse.ciphertextBlob().asByteArray();
        return Base64.getEncoder()
                .withoutPadding()
                .encodeToString(encryptedDataKey);
    }
}
