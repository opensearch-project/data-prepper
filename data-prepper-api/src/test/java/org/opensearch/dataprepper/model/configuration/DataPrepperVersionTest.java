package org.opensearch.dataprepper.model.configuration;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataPrepperVersionTest {

    @ParameterizedTest
    @MethodSource("validDataPrepperVersions")
    public void testValidVersionsCanBeParsedSuccessfully(final String version, final int expectedMajorVersion, final Optional<Integer> expectedMinorVersion) {
        final DataPrepperVersion result = DataPrepperVersion.parse(version);
        assertThat(result, notNullValue());
        assertThat(result.getMajorVersion(), is(equalTo(expectedMajorVersion)));
        assertThat(result.getMinorVersion(), is(equalTo(expectedMinorVersion)));
    }

    private static Stream<Arguments> validDataPrepperVersions() {
        return Stream.of(
            Arguments.of("1", 1, Optional.empty()),
            Arguments.of("1.0", 1, Optional.of(0)),
            Arguments.of("123423.0", 123423, Optional.of(0)),
            Arguments.of("2.325", 2, Optional.of(325)),
            Arguments.of("3.14", 3, Optional.of(14))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"*", "1.*", ".0", "alpha", "4.text", "foo.645", "\\a323"})
    public void testInvalidVersionsCannotBeParsed(final String version) {
        assertThrows(IllegalArgumentException.class, () -> DataPrepperVersion.parse(version));
    }

    @ParameterizedTest
    @MethodSource("compatibleDataPrepperVersions")
    public void testCompatibleWith_equalVersions(final String versionA, final String versionB) {
        final DataPrepperVersion resultA = DataPrepperVersion.parse(versionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(versionB);
        assertThat(resultA, notNullValue());
        assertThat(resultB, notNullValue());
        assertThat(resultA.compatibleWith(resultB), is(equalTo(true)));
    }

    private static Stream<Arguments> compatibleDataPrepperVersions() {
        return Stream.of(
            Arguments.of("1", "1.6"),
            Arguments.of("1.4", "1"),
            Arguments.of("1.4", "1.2"),
            Arguments.of("342.0", "342.0"),
            Arguments.of("342.8", "342.8"),
            Arguments.of("13", "13"),
            Arguments.of("13", "2.0"),
            Arguments.of("7.0", "5"),
            Arguments.of("42.0", "34.0"),
            Arguments.of("13", "11")
        );
    }

    @ParameterizedTest
    @MethodSource("nonCompatibleDataPrepperVersions")
    public void testCompatibleWith_lessThanVersions(final String versionA, final String versionB) {
        final DataPrepperVersion resultA = DataPrepperVersion.parse(versionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(versionB);
        assertThat(resultA, notNullValue());
        assertThat(resultB, notNullValue());
        assertThat(resultA.compatibleWith(resultB), is(false));
    }

    private static Stream<Arguments> nonCompatibleDataPrepperVersions() {
        return Stream.of(
            Arguments.of("1.2", "1.6"),
            Arguments.of("1", "2.0"),
            Arguments.of("2.0", "5"),
            Arguments.of("42.0", "343.0"),
            Arguments.of("42.0", "42.1"),
            Arguments.of("13", "15")
        );
    }

    @Test
    public void testEquals_withEqualShorthandVersions() {
        final String randVersion = RandomStringUtils.randomNumeric(4);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersion);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersion);
        assertThat(resultA.equals(resultB), is(equalTo(true)));
    }

    @Test
    public void testEquals_withEqualVersions() {
        final Random random = new Random();
        final int randMajorVersion = random.nextInt(100000);
        final int randMinorVersion = random.nextInt(100000);
        final String randVersion = String.format("%d.%d", randMajorVersion, randMinorVersion);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersion);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersion);
        assertThat(resultA.equals(resultB), is(equalTo(true)));
    }

    @Test
    public void testEquals_withNonEquivalentShorthandVersions() {
        final String randVersionA = RandomStringUtils.randomNumeric(4);
        final String randVersionB = RandomStringUtils.randomNumeric(2);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersionB);
        assertThat(resultA.equals(resultB), is(equalTo(false)));
    }

    @Test
    public void testEquals_withRandomNonEquivalentVersions() {
        final Random random = new Random();
        final int randMajorVersionA = random.nextInt(50000);
        int randMajorVersionB = random.nextInt(50000);
        if (randMajorVersionA == randMajorVersionB) {
            randMajorVersionB--;
        }
        final int randMinorVersionA = random.nextInt( 50000);
        final int randMinorVersionB = random.nextInt(50000);
        final String randVersionA = String.format("%d.%d", randMajorVersionA, randMinorVersionA);
        final String randVersionB = String.format("%d.%d", randMajorVersionB, randMinorVersionB);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersionB);
        assertThat(resultA.equals(resultB), is(equalTo(false)));
    }

    @ParameterizedTest
    @MethodSource("nonCompatibleDataPrepperVersions")
    public void testEquals_withNonEquivalentVersions(final String versionA, final String versionB) {
        final DataPrepperVersion resultA = DataPrepperVersion.parse(versionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(versionB);
        assertThat(resultA, notNullValue());
        assertThat(resultB, notNullValue());
        assertThat(resultA.equals(resultB), is(false));
    }

    @Test
    public void testEquals_withDifferentObject() {
        final DataPrepperVersion currentVersion = DataPrepperVersion.getCurrentVersion();
        assertThat(currentVersion.equals("foo"), is(false));
    }

    @Test
    public void testGetCurrentVersionIsNotNull() {
        final DataPrepperVersion currentVersion = DataPrepperVersion.getCurrentVersion();
        assertThat(currentVersion, notNullValue());
    }

    @Test
    public void testGetCurrentVersionAreAlwaysEqual() {
        final DataPrepperVersion currentVersion = DataPrepperVersion.getCurrentVersion();
        assertThat(currentVersion, notNullValue());
        final DataPrepperVersion currentVersion2 = DataPrepperVersion.getCurrentVersion();
        assertThat(currentVersion2, notNullValue());
        assertThat(currentVersion, is(equalTo(currentVersion2)));
    }

    @Test
    public void testToString_shorthandVersion() {
        final DataPrepperVersion result = DataPrepperVersion.parse("2");
        assertThat(result.toString(), is(equalTo("2")));
    }

    @Test
    public void testToString_fullVersion() {
        final DataPrepperVersion result = DataPrepperVersion.parse("7.0");
        assertThat(result.toString(), is(equalTo("7.0")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "2.0", "3.14", "105435"})
    public void testHashCode_areEqualForSameVersion(final String version) {

        final DataPrepperVersion dpVersionA = DataPrepperVersion.parse(version);
        final int hashCodeA = dpVersionA.hashCode();
        final DataPrepperVersion dpVersionB = DataPrepperVersion.parse(version);
        final int hashCodeB = dpVersionB.hashCode();
        assertThat(hashCodeA, is(equalTo(hashCodeB)));
    }
}
