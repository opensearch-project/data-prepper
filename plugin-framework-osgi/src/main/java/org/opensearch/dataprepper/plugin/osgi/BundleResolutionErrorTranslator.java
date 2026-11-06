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

import org.osgi.framework.BundleException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates OSGi/Felix bundle resolution failure messages into human-readable
 * diagnostic strings that name the plugin and the missing or incompatible
 * imported package with its version range.
 * <p>
 * This class is pure (no state, no I/O) and intended to be easily unit-tested
 * with representative Felix failure messages.
 */
final class BundleResolutionErrorTranslator {

    /**
     * Pattern matching the Felix "Unable to resolve" format:
     * {@code Unable to resolve <bundle-id> revision <rev>: missing requirement [<bundle-id> revision <rev>]
     * osgi.wiring.package; (&(osgi.wiring.package=<pkg>)(version>=<min>)(!(version>=<max>)))}
     */
    private static final Pattern UNABLE_TO_RESOLVE_PATTERN = Pattern.compile(
            "Unable to resolve\\b.*?missing requirement\\s*\\[.*?\\]\\s*"
                    + "osgi\\.wiring\\.package;\\s*"
                    + "\\(&\\(osgi\\.wiring\\.package=([^)]+)\\)"
                    + "\\(version>=([^)]+)\\)"
                    + "\\(!\\(version>=([^)]+)\\)\\)\\)",
            Pattern.DOTALL);

    /**
     * Simpler pattern for the "Unable to resolve ... missing requirement ... osgi.wiring.package=<pkg>"
     * without explicit version constraints.
     */
    private static final Pattern MISSING_PACKAGE_PATTERN = Pattern.compile(
            "Unable to resolve\\b.*?missing requirement\\s*\\[.*?\\]\\s*"
                    + "osgi\\.wiring\\.package;\\s*"
                    + "(?:\\(&)?\\(?osgi\\.wiring\\.package=([^)]+)\\)?",
            Pattern.DOTALL);

    /**
     * Pattern for Felix resolution errors with "filter" syntax:
     * {@code ... filter:="(&(osgi.wiring.package=<pkg>)(version>=<min>)(!(version>=<max>)))"}
     */
    private static final Pattern FILTER_PATTERN = Pattern.compile(
            "osgi\\.wiring\\.package.*?"
                    + "filter:?=?\"?\\(&\\(osgi\\.wiring\\.package=([^)]+)\\)"
                    + "\\(version>=([^)]+)\\)"
                    + "\\(!\\(version>=([^)]+)\\)\\)\\)",
            Pattern.DOTALL);

    private BundleResolutionErrorTranslator() {
    }

    /**
     * Translates a {@link BundleException} from a Felix resolution failure into
     * a human-readable message.
     *
     * @param bundleSymbolicName the symbolic name of the bundle that failed to resolve
     * @param exception the bundle exception (may be null)
     * @return a human-readable diagnostic string (never null)
     */
    static String translate(final String bundleSymbolicName, final BundleException exception) {
        if (exception == null) {
            return formatGeneric(bundleSymbolicName, "unknown error (null exception)");
        }

        final String message = exception.getMessage();
        if (message == null || message.isEmpty()) {
            return formatGeneric(bundleSymbolicName, "no details available");
        }

        return translateMessage(bundleSymbolicName, message);
    }

    /**
     * Translates a raw Felix resolution failure message string into a
     * human-readable message.
     *
     * @param bundleSymbolicName the symbolic name of the bundle
     * @param rawMessage the raw Felix error message
     * @return a human-readable diagnostic string (never null)
     */
    static String translateMessage(final String bundleSymbolicName, final String rawMessage) {
        if (rawMessage == null || rawMessage.isEmpty()) {
            return formatGeneric(bundleSymbolicName, "no details available");
        }

        // Try version-constrained pattern first (most informative)
        final Matcher versionMatcher = UNABLE_TO_RESOLVE_PATTERN.matcher(rawMessage);
        if (versionMatcher.find()) {
            return formatVersionMismatch(bundleSymbolicName,
                    versionMatcher.group(1),
                    versionMatcher.group(2),
                    versionMatcher.group(3));
        }

        // Try filter-based pattern
        final Matcher filterMatcher = FILTER_PATTERN.matcher(rawMessage);
        if (filterMatcher.find()) {
            return formatVersionMismatch(bundleSymbolicName,
                    filterMatcher.group(1),
                    filterMatcher.group(2),
                    filterMatcher.group(3));
        }

        // Try simple missing-package pattern (no version info)
        final Matcher missingMatcher = MISSING_PACKAGE_PATTERN.matcher(rawMessage);
        if (missingMatcher.find()) {
            return formatMissingPackage(bundleSymbolicName, missingMatcher.group(1));
        }

        // Fall back to a clean generic message
        return formatGeneric(bundleSymbolicName, rawMessage);
    }

    private static String formatVersionMismatch(final String bundleSymbolicName,
                                                final String packageName,
                                                final String minVersion,
                                                final String maxVersion) {
        return String.format(
                "Plugin '%s' could not be resolved: it requires package %s;version=[%s,%s) "
                        + "but the host/other bundles do not provide a compatible version.",
                bundleSymbolicName, packageName, minVersion, maxVersion);
    }

    private static String formatMissingPackage(final String bundleSymbolicName,
                                               final String packageName) {
        return String.format(
                "Plugin '%s' could not be resolved: it requires package %s "
                        + "which is not exported by the host or any other bundle.",
                bundleSymbolicName, packageName);
    }

    private static String formatGeneric(final String bundleSymbolicName, final String details) {
        return String.format(
                "Plugin '%s' could not be resolved: %s",
                bundleSymbolicName, details);
    }
}
