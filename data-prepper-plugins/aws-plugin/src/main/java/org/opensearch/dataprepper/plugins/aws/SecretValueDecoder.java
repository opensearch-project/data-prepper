/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class SecretValueDecoder {
    public String decode(final GetSecretValueResponse getSecretValueResponse) {
        if (getSecretValueResponse.secretString() != null) {
            return getSecretValueResponse.secretString();
        } else {
            return getSecretValueResponse.secretBinary().asUtf8String();
        }
    }
}
