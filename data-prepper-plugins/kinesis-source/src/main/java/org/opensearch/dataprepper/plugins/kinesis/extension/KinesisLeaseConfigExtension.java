/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.extension;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

@DataPrepperExtensionPlugin(modelType = KinesisLeaseConfig.class, rootKeyJsonPath = "/kinesis", allowInPipelineConfigurations = true)
public class KinesisLeaseConfigExtension implements ExtensionPlugin {

    private KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;
    @DataPrepperPluginConstructor
    public KinesisLeaseConfigExtension(final KinesisLeaseConfig kinesisLeaseConfig) {
        this.kinesisLeaseConfigSupplier = new KinesisLeaseConfigSupplier(kinesisLeaseConfig);
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new KinesisLeaseConfigProvider(this.kinesisLeaseConfigSupplier));
    }
}
