package org.opensearch.dataprepper.aws.api;

public interface SecretsSupplier {
    String retrieveValue(String secretId, String key);

    String retrieveValue(String secretId);
}
