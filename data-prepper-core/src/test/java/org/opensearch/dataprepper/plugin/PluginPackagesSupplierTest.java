/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class PluginPackagesSupplierTest {

    private static final String PROPERTIES_PREFIX = "org.opensearch.dataprepper.plugin.packages=";
    private static final String DEFAULT_PACKAGE_NAME = "org.opensearch.dataprepper.plugins";
    // TODO: Remove this once all plugins have migrated to org.opensearch
    private static final String DEPRECATED_DEFAULT_PACKAGE_NAME = "com.amazon.dataprepper.plugins";

    private PluginPackagesSupplier createObjectUnderTest() {
        final PluginPackagesSupplier object = new PluginPackagesSupplier();
        return spy(object);
    }

    @Test
    void get_returns_default_package_if_getResources_throws_exception() throws IOException {
        final PluginPackagesSupplier objectUnderTest = createObjectUnderTest();

        when(objectUnderTest.loadResources()).thenThrow(IOException.class);

        final String[] actualPackages = objectUnderTest.get();
        assertThat(actualPackages, notNullValue());

        assertThat(actualPackages.length, equalTo(2));
        assertThat(actualPackages[0], equalTo(DEFAULT_PACKAGE_NAME));
        assertThat(actualPackages[1], equalTo(DEPRECATED_DEFAULT_PACKAGE_NAME));
    }

    @Test
    void get_returns_default_package_if_no_resources_are_found() throws IOException {
        final PluginPackagesSupplier objectUnderTest = createObjectUnderTest();

        when(objectUnderTest.loadResources()).thenReturn(Collections.emptyIterator());

        final String[] actualPackages = objectUnderTest.get();
        assertThat(actualPackages, notNullValue());

        assertThat(actualPackages.length, equalTo(2));
        assertThat(actualPackages[0], equalTo(DEFAULT_PACKAGE_NAME));
        assertThat(actualPackages[1], equalTo(DEPRECATED_DEFAULT_PACKAGE_NAME));
    }

    @Test
    void get_returns_default_package_if_resources_have_no_plugin_packages_defined() throws IOException {
        final PluginPackagesSupplier objectUnderTest = createObjectUnderTest();

        final Iterator<URL> iteratorOfEmptyFiles = IntStream.range(0, 3)
                .mapToObj(propertyLine -> new ByteArrayInputStream(new byte[]{}))
                .map(PluginPackagesSupplierTest::createMockedUrl)
                .iterator();

        when(objectUnderTest.loadResources()).thenReturn(iteratorOfEmptyFiles);

        final String[] actualPackages = objectUnderTest.get();
        assertThat(actualPackages, notNullValue());

        assertThat(actualPackages.length, equalTo(2));
        assertThat(actualPackages[0], equalTo(DEFAULT_PACKAGE_NAME));
        assertThat(actualPackages[1], equalTo(DEPRECATED_DEFAULT_PACKAGE_NAME));
    }

    @Test
    void get_returns_default_package_if_resources_have_empty_plugin_packages_defined() throws IOException {
        final PluginPackagesSupplier objectUnderTest = createObjectUnderTest();

        final Iterator<URL> iterator = IntStream.range(0, 3)
                .mapToObj(packageName -> PROPERTIES_PREFIX)
                .map(propertyLine -> new ByteArrayInputStream(propertyLine.getBytes()))
                .map(PluginPackagesSupplierTest::createMockedUrl)
                .iterator();

        when(objectUnderTest.loadResources()).thenReturn(iterator);

        final String[] actualPackages = objectUnderTest.get();
        assertThat(actualPackages, notNullValue());

        assertThat(actualPackages.length, equalTo(2));
        assertThat(actualPackages[0], equalTo(DEFAULT_PACKAGE_NAME));
        assertThat(actualPackages[1], equalTo(DEPRECATED_DEFAULT_PACKAGE_NAME));
    }

    @Test
    void get_returns_packages_from_all_resources() throws IOException {
        final PluginPackagesSupplier objectUnderTest = createObjectUnderTest();

        final Set<String> packageNames = IntStream.range(0, 3)
                .mapToObj(i -> randomPackageName())
                .collect(Collectors.toSet());

        final Iterator<URL> iterator = packageNames.stream()
                .map(packageName -> PROPERTIES_PREFIX + packageName)
                .map(propertyLine -> new ByteArrayInputStream(propertyLine.getBytes()))
                .map(PluginPackagesSupplierTest::createMockedUrl)
                .iterator();

        when(objectUnderTest.loadResources()).thenReturn(iterator);

        final String[] actualPackages = objectUnderTest.get();
        assertThat(actualPackages, notNullValue());

        assertThat(actualPackages.length, equalTo(packageNames.size()));

        final Set<String> actualPackagesList = new HashSet<>(Arrays.asList(actualPackages));

        assertThat(actualPackagesList, equalTo(packageNames));
    }

    @Test
    void get_returns_multiple_packages_from_resources_files_with_comma_delimited_list() throws IOException {
        final PluginPackagesSupplier objectUnderTest = createObjectUnderTest();

        final Set<String> packageNames = IntStream.range(0, 3)
                .mapToObj(i -> randomPackageName())
                .collect(Collectors.toSet());

        final String propertyLine = PROPERTIES_PREFIX + String.join(",", packageNames);
        final InputStream inputStream = new ByteArrayInputStream(propertyLine.getBytes());
        final URL resourceUrl = createMockedUrl(inputStream);

        final Iterator<URL> iterator = Collections.singleton(resourceUrl).iterator();

        when(objectUnderTest.loadResources()).thenReturn(iterator);

        final String[] actualPackages = objectUnderTest.get();
        assertThat(actualPackages, notNullValue());

        assertThat(actualPackages.length, equalTo(packageNames.size()));

        final Set<String> actualPackagesList = new HashSet<>(Arrays.asList(actualPackages));

        assertThat(actualPackagesList, equalTo(packageNames));
    }

    private String randomPackageName() {
        return UUID.randomUUID().toString().replace("-", ".");
    }

    private static URL createMockedUrl(final InputStream inputStream) {
        final URL resourceUrl = mock(URL.class);
        try {
            when(resourceUrl.openStream()).thenReturn(inputStream);
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
        return resourceUrl;
    }
}