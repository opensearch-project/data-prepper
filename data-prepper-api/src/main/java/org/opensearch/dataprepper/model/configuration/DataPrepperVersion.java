package org.opensearch.dataprepper.model.configuration;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataPrepperVersion {
    private static final String CURRENT_VERSION = "2.8";

    private static final String FULL_FORMAT = "%d.%d";
    private static final String SHORTHAND_FORMAT = "%d";
    private static final String VERSION_PATTERN_STRING = "^((\\d+)(\\.(\\d+))?)$";
    private static final Pattern VERSION_PATTERN = Pattern.compile(VERSION_PATTERN_STRING);
    private static final int MAJOR_VERSION_PATTERN_POSITION = 2;
    private static final int MINOR_VERSION_PATTERN_POSITION = 4;

    private final int majorVersion;
    private final Integer minorVersion;

    private static DataPrepperVersion instance;

    private DataPrepperVersion(int majorVersion, Integer minorVersion) {
        this.minorVersion = minorVersion;
        this.majorVersion = majorVersion;
    }

    public static DataPrepperVersion getCurrentVersion() {
        if (Objects.isNull(instance)) {
            instance = parse(CURRENT_VERSION);
        }
        return instance;
    }

    public static DataPrepperVersion parse(final String version) {

        final Matcher result = VERSION_PATTERN.matcher(version);
        if(result.find()) {
            String major = result.group(MAJOR_VERSION_PATTERN_POSITION);
            final int foundMajorVersion = Integer.parseInt(major);
            final String potentialMinorVersion = result.group(MINOR_VERSION_PATTERN_POSITION);
            final Integer foundMinorVersion = potentialMinorVersion == null ? null : Integer.parseInt(potentialMinorVersion);

            return new DataPrepperVersion(foundMajorVersion, foundMinorVersion);
        }

        throw new IllegalArgumentException("Invalid Data Prepper Version provided: " + version);
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public Optional<Integer> getMinorVersion() {
        return Optional.ofNullable(minorVersion);
    }

    /**
     * Determines if the provided Data Prepper Version is compatible with this.
     *
     * The initial implementation is a basic implementation that will return true only when the comparing version is
     * less than or equal to this version.
     *
     * @param o - the other DataPrepperVersion to compare with
     * @return return true if the versions are compatible, otherwise false
     * @since 2.1
     */
    public boolean compatibleWith(DataPrepperVersion o) {

        if (this.majorVersion < o.getMajorVersion()) {
            return false;
        }

        if ((this.majorVersion == o.majorVersion) && (this.minorVersion != null && o.getMinorVersion().isPresent())) {
            if (!this.minorVersion.equals(o.getMinorVersion().get())) {
                return this.minorVersion > o.getMinorVersion().get();
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DataPrepperVersion) {
            final DataPrepperVersion other = (DataPrepperVersion) o;
            return this.majorVersion == other.majorVersion && Objects.equals(this.minorVersion, other.getMinorVersion().orElse(null));
        }
        return false;
    }

    @Override
    public String toString() {
        if (this.minorVersion == null) {
            return String.format(SHORTHAND_FORMAT, this.majorVersion);
        }

        return String.format(FULL_FORMAT, this.majorVersion, this.minorVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.majorVersion, this.minorVersion);
    }
}
