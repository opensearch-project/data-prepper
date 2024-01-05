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
