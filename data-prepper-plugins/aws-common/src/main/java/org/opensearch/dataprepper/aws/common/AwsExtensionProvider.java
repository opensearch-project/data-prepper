/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.common;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

public class AwsExtensionProvider implements ExtensionProvider<AwsCredentialsSupplier> {
    private final AwsCredentialsSupplier awsCredentialsSupplier;

    public AwsExtensionProvider(final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public Optional<AwsCredentialsSupplier> provideInstance(final Context context) {
        return Optional.of(awsCredentialsSupplier);
    }

    @Override
    public Class<AwsCredentialsSupplier> supportedClass() {
        return AwsCredentialsSupplier.class;
    }
}
