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

import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Provides the set of Data Prepper API packages exported to OSGi bundles via
 * {@code org.osgi.framework.system.packages.extra}. The package list is generated
 * at build time by scanning the data-prepper-api source tree and written to
 * a resource file ({@code META-INF/osgi-shared-packages.properties}).
 * <p>
 * Rule: include every {@code org.opensearch.dataprepper.*} package EXCEPT
 * {@code org.opensearch.dataprepper.plugins.*} (reserved for plugin implementations).
 * Third-party packages that cross the host/plugin boundary (Jackson annotations,
 * Jakarta validation) are also included.
 */
final class DataPrepperOsgiPackages {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperOsgiPackages.class);

    private static final String PACKAGES_RESOURCE = "META-INF/osgi-shared-packages.properties";
    private static final String VERSION_RESOURCE = "META-INF/data-prepper-api.version.properties";
    private static final String DP_PACKAGES_KEY = "data-prepper.packages";
    private static final String THIRD_PARTY_PACKAGES_KEY = "third-party.packages";
    private static final String VERSION_PROPERTY_KEY = "data-prepper-api.version";

    /**
     * Pattern matching a Maven/Gradle version string: major.minor.micro[-qualifier]
     */
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:[.-](.+))?");

    static final String SLF4J_PACKAGE = "org.slf4j;version=\"2.0.6\"";

    /**
     * The OSGi plugin framework package itself, exported so that adapted bundles
     * can resolve the {@link LegacyPluginBundleActivator} class set as their
     * Bundle-Activator.
     */
    static final String FRAMEWORK_PACKAGE = "org.opensearch.dataprepper.plugin.osgi";

    private DataPrepperOsgiPackages() {
    }

    /**
     * Lazily cached shared-packages properties loaded from the classpath resource.
     * Uses a holder class for thread-safe lazy initialization without synchronization.
     */
    private static class PackagesHolder {
        static final Properties PROPERTIES = loadPackagesResource();

        private static Properties loadPackagesResource() {
            final Properties props = new Properties();
            try (InputStream is = DataPrepperOsgiPackages.class.getClassLoader()
                    .getResourceAsStream(PACKAGES_RESOURCE)) {
                if (is != null) {
                    props.load(is);
                } else {
                    LOG.error("Resource {} not found on classpath. The Gradle build may not have generated it.",
                            PACKAGES_RESOURCE);
                }
            } catch (final IOException e) {
                LOG.error("Failed to read shared packages resource: {}", e.getMessage());
            }
            return props;
        }
    }

    /**
     * Builds the {@code org.osgi.framework.system.packages.extra} value for Felix.
     * Each Data Prepper API package is annotated with a version attribute derived
     * from the data-prepper-api build version (resolved via {@link DataPrepperVersion}).
     * The slf4j package is exported without a Data Prepper version. The plugin
     * framework package is also exported so adapted bundles can load the
     * LegacyPluginBundleActivator.
     *
     * @return a comma-separated list of versioned package exports
     */
    static String buildSystemPackagesExtra() {
        final String apiVersion = resolveOsgiVersion();
        final List<String> dataPrepperPackages = loadDataPrepperPackages();
        final List<String> thirdPartyPackages = loadThirdPartyPackages();

        LOG.info("OSGi shared packages: {} Data Prepper packages + {} third-party packages, version={}",
                dataPrepperPackages.size(), thirdPartyPackages.size(), apiVersion);

        final String versionedDpPackages = dataPrepperPackages.stream()
                .map(pkg -> pkg + ";version=\"" + apiVersion + "\"")
                .collect(Collectors.joining(","));

        final String thirdPartyExports = String.join(",", thirdPartyPackages);

        final List<String> segments = new ArrayList<>(4);
        if (!versionedDpPackages.isEmpty()) {
            segments.add(versionedDpPackages);
        }
        if (!thirdPartyExports.isEmpty()) {
            segments.add(thirdPartyExports);
        }
        segments.add(SLF4J_PACKAGE);
        segments.add(FRAMEWORK_PACKAGE);

        return String.join(",", segments);
    }

    /**
     * Returns the list of Data Prepper packages shared with OSGi bundles,
     * loaded from the build-generated resource.
     */
    static List<String> loadDataPrepperPackages() {
        final Properties props = PackagesHolder.PROPERTIES;
        final String packagesValue = props.getProperty(DP_PACKAGES_KEY);
        if (packagesValue == null || packagesValue.isEmpty()) {
            LOG.error("No Data Prepper shared packages found in resource {}. "
                    + "Ensure the generateSharedPackagesResource Gradle task ran.", PACKAGES_RESOURCE);
            return Collections.emptyList();
        }
        return Arrays.stream(packagesValue.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Returns the list of third-party packages shared with OSGi bundles.
     */
    static List<String> loadThirdPartyPackages() {
        final Properties props = PackagesHolder.PROPERTIES;
        final String packagesValue = props.getProperty(THIRD_PARTY_PACKAGES_KEY);
        if (packagesValue == null || packagesValue.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(packagesValue.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Resolves the Data Prepper version in valid OSGi format (major.minor.micro).
     * Delegates to {@link DataPrepperVersion#getCurrentVersion()} when available,
     * with a fallback to the build-generated version resource for environments
     * where the SPI is not on the classpath (e.g. isolated unit tests).
     *
     * @return a version string such as {@code "2.16.0"} (never null)
     */
    static String resolveOsgiVersion() {
        // Try DataPrepperVersion via SPI first (available at runtime)
        try {
            final DataPrepperVersion version = DataPrepperVersion.getCurrentVersion();
            final int major = version.getMajorVersion();
            final int minor = version.getMinorVersion().orElse(0);
            final String osgiVersion = major + "." + minor + ".0";
            LOG.debug("Resolved OSGi version from DataPrepperVersion: {}", osgiVersion);
            return osgiVersion;
        } catch (final RuntimeException e) {
            LOG.debug("DataPrepperVersion SPI not available, falling back to version resource: {}",
                    e.getMessage());
        }

        // Fallback: read from build-generated resource
        return resolveVersionFromResource();
    }

    /**
     * Reads the version from the build-generated properties resource and converts
     * to OSGi format (strips SNAPSHOT, ensures three-part version).
     */
    private static String resolveVersionFromResource() {
        try (InputStream is = DataPrepperOsgiPackages.class.getClassLoader()
                .getResourceAsStream(VERSION_RESOURCE)) {
            if (is != null) {
                final Properties props = new Properties();
                props.load(is);
                final String raw = props.getProperty(VERSION_PROPERTY_KEY);
                if (raw != null && !raw.isEmpty()) {
                    final String osgiVersion = toOsgiVersion(raw);
                    LOG.debug("Resolved OSGi version from resource: {}", osgiVersion);
                    return osgiVersion;
                }
            }
        } catch (final IOException e) {
            LOG.debug("Failed to read version resource: {}", e.getMessage());
        }
        LOG.warn("Could not resolve Data Prepper version; using fallback 0.0.0");
        return "0.0.0";
    }

    /**
     * Converts a Maven/Gradle version string to a valid OSGi version.
     * Strips {@code -SNAPSHOT} and ensures three-part format (major.minor.micro).
     */
    static String toOsgiVersion(final String version) {
        if (version == null || version.isEmpty()) {
            return "0.0.0";
        }
        final Matcher matcher = VERSION_PATTERN.matcher(version);
        if (!matcher.matches()) {
            return "0.0.0";
        }
        final String major = matcher.group(1);
        final String minor = matcher.group(2);
        final String micro = matcher.group(3) != null ? matcher.group(3) : "0";
        return major + "." + minor + "." + micro;
    }
}
