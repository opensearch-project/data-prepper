/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperVersionTest {
    @Mock
    private ServiceLoader<VersionProvider> serviceLoader;
    @Mock
    private VersionProvider currentVersionProvider;

    @AfterEach
    void resetCurrentVersion() throws IllegalAccessException, NoSuchFieldException {
        final Field instanceField = DataPrepperVersion.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
        instanceField.setAccessible(false);
    }

    @ParameterizedTest
    @MethodSource("validDataPrepperVersions")
    void testValidVersionsCanBeParsedSuccessfully(final String version, final int expectedMajorVersion, final Optional<Integer> expectedMinorVersion) {
        final DataPrepperVersion result = DataPrepperVersion.parse(version);
        assertThat(result, notNullValue());
        assertThat(result.getMajorVersion(), is(equalTo(expectedMajorVersion)));
        assertThat(result.getMinorVersion(), is(equalTo(expectedMinorVersion)));
    }

    private static Stream<Arguments> validDataPrepperVersions() {
        return Stream.of(
            arguments("1", 1, Optional.empty()),
            arguments("1.0", 1, Optional.of(0)),
            arguments("123423.0", 123423, Optional.of(0)),
            arguments("2.325", 2, Optional.of(325)),
            arguments("3.14", 3, Optional.of(14))
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"*", "1.*", ".0", "alpha", "4.text", "foo.645",
            "\\a323", "1.1.1.1", "1.1.1-RELEASE", "1.1-RELEASE", "1-test"})
    void testInvalidVersionsCannotBeParsed(final String version) {
        assertThrows(IllegalArgumentException.class, () -> DataPrepperVersion.parse(version));
    }

    @ParameterizedTest
    @ValueSource(strings = {"3.14.1", "3.14.1-SNAPSHOT", "11-SNAPSHOT"})
    void parse_throws_if_given_a_valid_version_that_cannot_be_a_configuration_version(final String version) {
        assertThrows(IllegalArgumentException.class, () -> DataPrepperVersion.parse(version));
    }

    @ParameterizedTest
    @MethodSource("compatibleDataPrepperVersions")
    void testCompatibleWith_equalVersions(final String versionA, final String versionB) {
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
    void testCompatibleWith_lessThanVersions(final String versionA, final String versionB) {
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
    void testEquals_withEqualShorthandVersions() {
        final String randVersion = RandomStringUtils.randomNumeric(4);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersion);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersion);
        assertThat(resultA.equals(resultB), is(equalTo(true)));
    }

    @Test
    void testEquals_withEqualVersions() {
        final Random random = new Random();
        final int randMajorVersion = random.nextInt(100000);
        final int randMinorVersion = random.nextInt(100000);
        final String randVersion = String.format("%d.%d", randMajorVersion, randMinorVersion);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersion);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersion);
        assertThat(resultA.equals(resultB), is(equalTo(true)));
    }

    @Test
    void testEquals_withNonEquivalentShorthandVersions() {
        final String randVersionA = RandomStringUtils.randomNumeric(4);
        final String randVersionB = RandomStringUtils.randomNumeric(2);
        final DataPrepperVersion resultA = DataPrepperVersion.parse(randVersionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(randVersionB);
        assertThat(resultA.equals(resultB), is(equalTo(false)));
    }

    @Test
    void testEquals_withRandomNonEquivalentVersions() {
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
    void testEquals_withNonEquivalentVersions(final String versionA, final String versionB) {
        final DataPrepperVersion resultA = DataPrepperVersion.parse(versionA);
        final DataPrepperVersion resultB = DataPrepperVersion.parse(versionB);
        assertThat(resultA, notNullValue());
        assertThat(resultB, notNullValue());
        assertThat(resultA.equals(resultB), is(false));
    }

    @Test
    void testEquals_withDifferentObject() {
        final DataPrepperVersion currentVersion = DataPrepperVersion.parse("1.2");
        assertThat(currentVersion.equals("1.2"), is(false));
    }

    @ParameterizedTest
    @CsvSource({
            "2.11, 2.11",
            "2.13, 2.13",
            "3.0, 3.0",
            "3.1, 3.1",
            "3.14.1, 3.14",
            "3.14.20, 3.14",
            "3.14.1-SNAPSHOT, 3.14",
            "11-SNAPSHOT, 11"})
    void getCurrentVersion_returns_value_from_VersionProvider(final String versionString, final String expectedVersion) {
        final DataPrepperVersion currentVersion;
        when(serviceLoader.findFirst()).thenReturn(Optional.of(currentVersionProvider));
        when(currentVersionProvider.getVersionString()).thenReturn(versionString);
        try (final MockedStatic<ServiceLoader> serviceLoaderStatic = mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(VersionProvider.class))
                    .thenReturn(serviceLoader);

            currentVersion = DataPrepperVersion.getCurrentVersion();
        }

        assertThat(currentVersion, is(notNullValue()));
        assertThat(currentVersion.toString(), is(equalTo(expectedVersion)));
    }

    @Test
    void getCurrentVersion_called_multiple_times_returns_same_instance() {
        final DataPrepperVersion currentVersion;
        when(serviceLoader.findFirst()).thenReturn(Optional.of(currentVersionProvider));
        when(currentVersionProvider.getVersionString()).thenReturn("2.11");
        try (final MockedStatic<ServiceLoader> serviceLoaderStatic = mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(VersionProvider.class))
                    .thenReturn(serviceLoader);

            currentVersion = DataPrepperVersion.getCurrentVersion();
        }

        assertThat(currentVersion, is(notNullValue()));
        assertThat(DataPrepperVersion.getCurrentVersion(), is(sameInstance(currentVersion)));
        assertThat(DataPrepperVersion.getCurrentVersion(), is(sameInstance(currentVersion)));
    }

    @Test
    void getCurrentVersion_throws_if_no_VersionProvider() {
        when(serviceLoader.findFirst()).thenReturn(Optional.empty());
        try (final MockedStatic<ServiceLoader> serviceLoaderStatic = mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(VersionProvider.class))
                    .thenReturn(serviceLoader);

            assertThrows(RuntimeException.class, DataPrepperVersion::getCurrentVersion);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"2.", ".", ".1", "3.1.2.11", "3.14.1-RELEASE", "3.14.1-snapshot"})
    void getCurrentVersion_throws_for_invalid_DataPrepperVersion(final String versionString) {
        when(serviceLoader.findFirst()).thenReturn(Optional.of(currentVersionProvider));
        when(currentVersionProvider.getVersionString()).thenReturn(versionString);
        try (final MockedStatic<ServiceLoader> serviceLoaderStatic = mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(VersionProvider.class))
                    .thenReturn(serviceLoader);

            assertThrows(IllegalArgumentException.class, DataPrepperVersion::getCurrentVersion);
        }
    }

    @Test
    void testToString_shorthandVersion() {
        final DataPrepperVersion result = DataPrepperVersion.parse("2");
        assertThat(result.toString(), is(equalTo("2")));
    }

    @Test
    void testToString_fullVersion() {
        final DataPrepperVersion result = DataPrepperVersion.parse("7.0");
        assertThat(result.toString(), is(equalTo("7.0")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "2.0", "3.14", "105435"})
    void testHashCode_areEqualForSameVersion(final String version) {

        final DataPrepperVersion dpVersionA = DataPrepperVersion.parse(version);
        final int hashCodeA = dpVersionA.hashCode();
        final DataPrepperVersion dpVersionB = DataPrepperVersion.parse(version);
        final int hashCodeB = dpVersionB.hashCode();
        assertThat(hashCodeA, is(equalTo(hashCodeB)));
    }
}
