/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.regions.Region;

import java.util.Map;
import java.util.Objects;

/**
 * Internal class to identify credentials. This is a distinct class from
 * {@link AwsCredentialsOptions} in order to ensure that the internal caching
 * is distinct from the external model.
 */
class CredentialsIdentifier {
    private final String stsRoleArn;
    private final Region region;
    private final Map<String, String> stsHeaderOverrides;

    private CredentialsIdentifier(
            final String stsRoleArn,
            final Region region,
            final Map<String, String> stsHeaderOverrides) {

        this.stsRoleArn = stsRoleArn;
        this.region = region;
        this.stsHeaderOverrides = stsHeaderOverrides;
    }

    static CredentialsIdentifier fromAwsCredentialsOption(final AwsCredentialsOptions options) {
        return new CredentialsIdentifier(
                options.getStsRoleArn(),
                options.getRegion(),
                options.getStsHeaderOverrides()
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final CredentialsIdentifier that = (CredentialsIdentifier) o;
        return Objects.equals(stsRoleArn, that.stsRoleArn) && Objects.equals(region, that.region) && Objects.equals(stsHeaderOverrides, that.stsHeaderOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stsRoleArn, region, stsHeaderOverrides);
    }
}
