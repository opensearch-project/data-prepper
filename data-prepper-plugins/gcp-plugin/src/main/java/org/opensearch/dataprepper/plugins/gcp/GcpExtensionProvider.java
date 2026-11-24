/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import org.opensearch.dataprepper.gcp.api.GcpCredentialsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

class GcpExtensionProvider implements ExtensionProvider<GcpCredentialsSupplier> {
    private final GcpCredentialsSupplier gcpCredentialsSupplier;

    GcpExtensionProvider(final GcpCredentialsSupplier gcpCredentialsSupplier) {
        this.gcpCredentialsSupplier = gcpCredentialsSupplier;
    }

    @Override
    public Optional<GcpCredentialsSupplier> provideInstance(final Context context) {
        return Optional.of(gcpCredentialsSupplier);
    }

    @Override
    public Class<GcpCredentialsSupplier> supportedClass() {
        return GcpCredentialsSupplier.class;
    }
}
