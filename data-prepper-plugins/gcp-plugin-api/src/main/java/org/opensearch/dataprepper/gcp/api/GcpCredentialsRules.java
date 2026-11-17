/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

/**
 * The credentials validation rules shared by {@link GcpConfig} and {@link GcpCredentialsOptions}.
 */
final class GcpCredentialsRules {
    static final String ADC_WITH_KEY_FIELDS =
            "auth_mode adc cannot be combined with private_key, client_email or token_uri";
    static final String KEY_FIELDS_WITHOUT_MODE =
            "private_key, client_email and token_uri require auth_mode service_account_key";
    static final String INCOMPLETE_SERVICE_ACCOUNT_KEY =
            "auth_mode service_account_key requires a non-blank private_key and client_email";

    private GcpCredentialsRules() {
    }

    static boolean isValidAdc(final GcpAuthMode authMode, final String privateKey, final String clientEmail,
                              final String tokenUri) {
        return authMode != GcpAuthMode.ADC || !hasKeyFields(privateKey, clientEmail, tokenUri);
    }

    static boolean isValidWithoutMode(final GcpAuthMode authMode, final String privateKey, final String clientEmail,
                                      final String tokenUri) {
        return authMode != null || !hasKeyFields(privateKey, clientEmail, tokenUri);
    }

    static boolean isCompleteServiceAccountKey(final GcpAuthMode authMode, final String privateKey, final String clientEmail) {
        return authMode != GcpAuthMode.SERVICE_ACCOUNT_KEY || (!isBlank(privateKey) && !isBlank(clientEmail));
    }

    private static boolean hasKeyFields(final String privateKey, final String clientEmail, final String tokenUri) {
        return privateKey != null || clientEmail != null || tokenUri != null;
    }

    private static boolean isBlank(final String value) {
        return value == null || value.trim().isEmpty();
    }
}
