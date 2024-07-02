/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsSecretsPluginConfigValueTranslator implements PluginConfigValueTranslator {
    static final String DEPRECATED_AWS_SECRETS_PREFIX = "aws_secrets";
    static final String AWS_SECRETS_PREFIX = "aws:secrets";
    static final String SECRET_CONFIGURATION_ID_GROUP = "secretConfigurationId";
    static final String SECRET_KEY_GROUP = "secretKey";
    static final Pattern SECRETS_REF_PATTERN = Pattern.compile(
            String.format("^(?<%s>[a-zA-Z0-9\\/_+.=@-]+)(:(?<%s>.+))?$",
                    SECRET_CONFIGURATION_ID_GROUP, SECRET_KEY_GROUP));

    private final SecretsSupplier secretsSupplier;

    public AwsSecretsPluginConfigValueTranslator(final SecretsSupplier secretsSupplier) {
        this.secretsSupplier = secretsSupplier;
    }

    @Override
    public Object translate(final String value) {
        final Matcher matcher = SECRETS_REF_PATTERN.matcher(value);
        if (matcher.matches()) {
            final String secretId = matcher.group(SECRET_CONFIGURATION_ID_GROUP);
            final String secretKey = matcher.group(SECRET_KEY_GROUP);
            return secretKey != null ? secretsSupplier.retrieveValue(secretId, secretKey) :
                    secretsSupplier.retrieveValue(secretId);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Unable to parse %s or %s according to pattern %s",
                    SECRET_CONFIGURATION_ID_GROUP, SECRET_KEY_GROUP, SECRETS_REF_PATTERN.pattern()));
        }
    }

    @Override
    public String getDeprecatedPrefix() {
        return DEPRECATED_AWS_SECRETS_PREFIX;
    }

    @Override
    public String getPrefix() {
        return AWS_SECRETS_PREFIX;
    }
}
