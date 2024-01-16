package org.opensearch.dataprepper.plugins.aws;

public interface SecretsSupplier {
    Object retrieveValue(String secretId, String key);

    Object retrieveValue(String secretId);

    void refresh(String secretId);
}
