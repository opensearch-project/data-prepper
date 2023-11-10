/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.Version;

/**
 * An interface for letting the test declare what OpenSearch version is being
 * tested against.
 */
class DeclaredOpenSearchVersion implements Comparable<DeclaredOpenSearchVersion> {
    private static final DeclaredOpenSearchVersion DEFAULT = new DeclaredOpenSearchVersion(Distribution.OPENSEARCH, "1.0.0");
    public static final DeclaredOpenSearchVersion OPENDISTRO_1_9 = new DeclaredOpenSearchVersion(Distribution.OPENDISTRO, "1.9.0");
    public static final DeclaredOpenSearchVersion OPENDISTRO_0_10 = new DeclaredOpenSearchVersion(Distribution.OPENDISTRO, "0.10.0");

    enum Distribution {
        OPENDISTRO,
        OPENSEARCH
    }

    private final Distribution distribution;
    private final String version;
    private final Version internalVersion;

    private DeclaredOpenSearchVersion(final Distribution distribution, final String version) {
        this.distribution = distribution;
        this.version = version;

        internalVersion = Version.fromString(version);
    }

    static DeclaredOpenSearchVersion parse(final String versionString) {
        if(versionString == null || versionString.isEmpty()) {
            return DEFAULT;
        }

        final String[] parts = versionString.split(":");

        if(parts.length != 2) {
            throw new IllegalArgumentException("Invalid version string provided.");
        }

        final Distribution distribution = Distribution.valueOf(parts[0].toUpperCase());

        return new DeclaredOpenSearchVersion(distribution, parts[1]);
    }

    @Override
    public int compareTo(final DeclaredOpenSearchVersion other) {
        final int distributionCompareTo = distribution.compareTo(other.distribution);

        if(distributionCompareTo != 0) {
            return distributionCompareTo;
        }

        return internalVersion.compareTo(other.internalVersion);
    }

    Distribution getDistribution() {
        return distribution;
    }

    String getVersion() {
        return version;
    }
}
