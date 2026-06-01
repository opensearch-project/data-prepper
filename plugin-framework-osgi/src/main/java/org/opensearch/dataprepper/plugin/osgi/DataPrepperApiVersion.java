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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the Data Prepper API version at runtime for use in OSGi package
 * export version attributes. The resolution strategy is:
 * <ol>
 *   <li>Read {@code Implementation-Version} from the manifest of the package
 *       containing {@code org.opensearch.dataprepper.model.event}.</li>
 *   <li>Read the version from a build-time generated properties resource
 *       ({@code META-INF/data-prepper-api.version.properties}).</li>
 *   <li>Fall back to a compile-time constant.</li>
 * </ol>
 * The resolved version is converted to valid OSGi format (major.minor.micro[.qualifier]).
 * Any {@code -SNAPSHOT} suffix is stripped or converted to an OSGi qualifier.
 */
final class DataPrepperApiVersion {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperApiVersion.class);

    static final String FALLBACK_VERSION = "2.15.0";

    private static final String VERSION_RESOURCE = "META-INF/data-prepper-api.version.properties";
    private static final String VERSION_PROPERTY_KEY = "data-prepper-api.version";

    /**
     * Pattern matching a Maven/Gradle version string: major.minor.micro[-qualifier]
     */
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:[.-](.+))?");

    private DataPrepperApiVersion() {
    }

    /**
     * Resolves the Data Prepper API version in OSGi format.
     *
     * @return a version string such as {@code "2.15.0"} (never null)
     */
    static String resolveVersion() {
        final String version = resolveRawVersion();
        return toOsgiVersion(version);
    }

    /**
     * Resolves the raw version string before OSGi normalization.
     */
    static String resolveRawVersion() {
        final String manifestVersion = readManifestVersion();
        if (manifestVersion != null && !manifestVersion.isEmpty()) {
            LOG.debug("Resolved Data Prepper API version from manifest: {}", manifestVersion);
            return manifestVersion;
        }

        final String resourceVersion = readResourceVersion();
        if (resourceVersion != null && !resourceVersion.isEmpty()) {
            LOG.debug("Resolved Data Prepper API version from resource: {}", resourceVersion);
            return resourceVersion;
        }

        LOG.warn("Could not resolve Data Prepper API version from manifest or resource; "
                + "using fallback: {}", FALLBACK_VERSION);
        return FALLBACK_VERSION;
    }

    /**
     * Reads the {@code Implementation-Version} from the JAR manifest of the package
     * that owns {@code org.opensearch.dataprepper.model.event}.
     */
    static String readManifestVersion() {
        try {
            final Class<?> eventClass = Class.forName("org.opensearch.dataprepper.model.event.Event");
            final Package pkg = eventClass.getPackage();
            if (pkg != null && pkg.getImplementationVersion() != null) {
                return pkg.getImplementationVersion();
            }
        } catch (final ClassNotFoundException e) {
            LOG.debug("Event class not found on classpath; manifest version unavailable");
        }
        return null;
    }

    /**
     * Reads the version from a build-time generated properties resource.
     */
    static String readResourceVersion() {
        try (InputStream is = DataPrepperApiVersion.class.getClassLoader()
                .getResourceAsStream(VERSION_RESOURCE)) {
            if (is != null) {
                final Properties props = new Properties();
                props.load(is);
                return props.getProperty(VERSION_PROPERTY_KEY);
            }
        } catch (final IOException e) {
            LOG.debug("Failed to read version resource: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Converts a Maven/Gradle version string to a valid OSGi version.
     * <ul>
     *   <li>{@code "2.15.0-SNAPSHOT"} becomes {@code "2.15.0"}</li>
     *   <li>{@code "2.15.0"} stays {@code "2.15.0"}</li>
     *   <li>{@code "2.15"} becomes {@code "2.15.0"}</li>
     * </ul>
     * The SNAPSHOT qualifier is stripped entirely (OSGi versions should not
     * include SNAPSHOT as it has no ordering semantics in OSGi).
     *
     * @param version the raw version string
     * @return a valid OSGi version string
     */
    static String toOsgiVersion(final String version) {
        if (version == null || version.isEmpty()) {
            return FALLBACK_VERSION;
        }

        final Matcher matcher = VERSION_PATTERN.matcher(version);
        if (!matcher.matches()) {
            LOG.warn("Version '{}' does not match expected pattern; using fallback", version);
            return FALLBACK_VERSION;
        }

        final String major = matcher.group(1);
        final String minor = matcher.group(2);
        final String micro = matcher.group(3) != null ? matcher.group(3) : "0";
        final String qualifier = matcher.group(4);

        if (qualifier == null || "SNAPSHOT".equalsIgnoreCase(qualifier)) {
            return major + "." + minor + "." + micro;
        }

        // Convert non-SNAPSHOT qualifier to valid OSGi qualifier (alphanumeric only)
        final String sanitizedQualifier = qualifier.replaceAll("[^a-zA-Z0-9]", "");
        if (sanitizedQualifier.isEmpty()) {
            return major + "." + minor + "." + micro;
        }
        return major + "." + minor + "." + micro + "." + sanitizedQualifier;
    }

    /**
     * Computes a semver-compatible import version range for a given export version.
     * The range is {@code [major.minor.micro, (major+1).0.0)} — meaning a plugin
     * built on version X.Y.Z will accept any version from X.Y.Z (inclusive) up to
     * but not including the next major version.
     *
     * @param osgiVersion a version in OSGi format (e.g. "2.15.0")
     * @return the version range string (e.g. "[2.15.0,3.0.0)")
     */
    static String computeImportRange(final String osgiVersion) {
        final Matcher matcher = VERSION_PATTERN.matcher(osgiVersion);
        if (!matcher.matches()) {
            return "[" + FALLBACK_VERSION + "," + (Integer.parseInt(FALLBACK_VERSION.split("\\.")[0]) + 1) + ".0.0)";
        }

        final int major = Integer.parseInt(matcher.group(1));
        final String minor = matcher.group(2);
        final String micro = matcher.group(3) != null ? matcher.group(3) : "0";

        return "[" + major + "." + minor + "." + micro + "," + (major + 1) + ".0.0)";
    }
}
