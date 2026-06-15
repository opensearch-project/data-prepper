/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Shared constants for Data Prepper API packages exported to OSGi bundles.
 */
final class DataPrepperOsgiPackages {
    private DataPrepperOsgiPackages() {}

    static final List<String> SHARED_API_PACKAGES = List.of(
            "org.opensearch.dataprepper.expression",
            "org.opensearch.dataprepper.logging",
            "org.opensearch.dataprepper.metrics",
            "org.opensearch.dataprepper.model.acknowledgements",
            "org.opensearch.dataprepper.model.annotations",
            "org.opensearch.dataprepper.model.breaker",
            "org.opensearch.dataprepper.model.buffer",
            "org.opensearch.dataprepper.model.codec",
            "org.opensearch.dataprepper.model.configuration",
            "org.opensearch.dataprepper.model.constraints",
            "org.opensearch.dataprepper.model.document",
            "org.opensearch.dataprepper.model.encryption",
            "org.opensearch.dataprepper.model.event",
            "org.opensearch.dataprepper.model.event.exceptions",
            "org.opensearch.dataprepper.model.failures",
            "org.opensearch.dataprepper.model.host",
            "org.opensearch.dataprepper.model.io",
            "org.opensearch.dataprepper.model.log",
            "org.opensearch.dataprepper.model.metric",
            "org.opensearch.dataprepper.model.opensearch",
            "org.opensearch.dataprepper.model.peerforwarder",
            "org.opensearch.dataprepper.model.pipeline",
            "org.opensearch.dataprepper.model.plugin",
            "org.opensearch.dataprepper.model.processor",
            "org.opensearch.dataprepper.model.record",
            "org.opensearch.dataprepper.model.sink",
            "org.opensearch.dataprepper.model.source",
            "org.opensearch.dataprepper.model.source.coordinator",
            "org.opensearch.dataprepper.model.source.coordinator.enhanced",
            "org.opensearch.dataprepper.model.source.coordinator.exceptions",
            "org.opensearch.dataprepper.model.source.s3",
            "org.opensearch.dataprepper.model.trace",
            "org.opensearch.dataprepper.model.types",
            "org.opensearch.dataprepper.model.validation",
            "org.opensearch.dataprepper.processor.state",
            "org.opensearch.dataprepper.typeconverter"
    );

    static final String SLF4J_PACKAGE = "org.slf4j";

    /**
     * The OSGi plugin framework package itself, exported so that adapted bundles
     * can resolve the {@link LegacyPluginBundleActivator} class set as their
     * Bundle-Activator.
     */
    static final String FRAMEWORK_PACKAGE = "org.opensearch.dataprepper.plugin.osgi";

    /**
     * Builds the {@code org.osgi.framework.system.packages.extra} value for Felix.
     * Each Data Prepper API package is annotated with a version attribute derived
     * from the data-prepper-api build version. The slf4j package is exported
     * without a Data Prepper version (it has its own versioning). The plugin
     * framework package is also exported so adapted bundles can load the
     * LegacyPluginBundleActivator.
     *
     * @return a comma-separated list of versioned package exports
     */
    static String buildSystemPackagesExtra() {
        final String apiVersion = DataPrepperApiVersion.resolveVersion();
        final String versionedApiPackages = SHARED_API_PACKAGES.stream()
                .map(pkg -> pkg + ";version=\"" + apiVersion + "\"")
                .collect(Collectors.joining(","));
        return versionedApiPackages + "," + SLF4J_PACKAGE + "," + FRAMEWORK_PACKAGE;
    }
}
