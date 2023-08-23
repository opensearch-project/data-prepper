package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.SecretsSupplier;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsSecretsPluginConfigValueTranslator implements PluginConfigValueTranslator {
    static final String SECRET_MANAGER_NAME_GROUP = "secretManager";
    static final String SECRET_KEY_GROUP = "secretKey";
    static final Pattern SECRETS_REF_PATTERN = Pattern.compile(
            String.format("^\\$\\[\\[aws_secrets\\.(?<%s>[a-zA-Z0-9\\/_+=.@-]+)\\.(?<%s>.+)\\]\\]$",
                    SECRET_MANAGER_NAME_GROUP, SECRET_KEY_GROUP));

    private final SecretsSupplier secretsSupplier;

    public AwsSecretsPluginConfigValueTranslator(final SecretsSupplier secretsSupplier) {
        this.secretsSupplier = secretsSupplier;
    }

    public String translate(final String value) {
        final Matcher matcher = SECRETS_REF_PATTERN.matcher(value);
        if (matcher.matches()) {
            final String secretManagerName = matcher.group(SECRET_MANAGER_NAME_GROUP);
            final String secretKey = matcher.group(SECRET_KEY_GROUP);
            return secretsSupplier.retrieveValue(secretManagerName, secretKey);
        } else {
            return value;
        }
    }
}
