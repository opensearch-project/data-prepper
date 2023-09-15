package org.opensearch.dataprepper.plugins.aws;

public interface SecretsSupplier {
    String retrieveValue(String secretId, String key);

    String retrieveValue(String secretId);
}
