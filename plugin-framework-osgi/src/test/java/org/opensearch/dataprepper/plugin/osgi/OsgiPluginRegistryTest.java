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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.ServiceReference;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OsgiPluginRegistryTest {

    /**
     * The LDAP filter {@link OsgiPluginRegistry} uses to select plugin services. Spelled out here
     * rather than shared with the production constant so that a change to the filter has to be a
     * deliberate change to this contract.
     */
    private static final String PLUGIN_SERVICE_FILTER = "(dataprepper.plugin.name=*)";

    private FelixPluginManager felixPluginManager;
    private OsgiPluginRegistry registry;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        registry = new OsgiPluginRegistry(felixPluginManager.getBundleContext());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void scanServices_with_no_services_produces_empty_map() {
        registry.scanServices();
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void findPluginClass_returns_empty_for_unknown_plugin() {
        assertThat(registry.findPluginClass(Object.class, "unknown").isPresent(), is(false));
    }

    @Test
    void findPluginClasses_returns_empty_for_unknown_type() {
        assertThat(registry.findPluginClasses(Object.class).isEmpty(), is(true));
    }

    @Test
    void getPluginCount_triggers_scan_on_first_call() {
        // First call should trigger scan
        final int count = registry.getPluginCount();
        assertThat(count, is(0));
        // Second call should use cached result
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void findPluginClass_triggers_scan_on_first_call() {
        registry.findPluginClass(Object.class, "test");
        // Should not throw, scan happens lazily
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void findPluginClasses_triggers_scan_on_first_call() {
        registry.findPluginClasses(Object.class);
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void refresh_invalidates_cache_and_next_call_rescans() {
        // First access triggers scan
        assertThat(registry.getPluginCount(), is(0));
        // Refresh clears cached state
        registry.refresh();
        // Next call should re-scan without error
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void refresh_is_thread_safe_with_concurrent_readers() throws Exception {
        // Trigger initial scan
        registry.getPluginCount();

        final int threadCount = 10;
        final int iterationsPerThread = 200;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int iteration = 0; iteration < iterationsPerThread; iteration++) {
                        if (index % 2 == 0) {
                            registry.refresh();
                        } else {
                            final Optional<?> result = registry.findPluginClass(Object.class, "test");
                            // Should never throw NPE from torn reads
                            assertThat(result.isPresent(), is(false));
                        }
                    }
                } catch (final Throwable t) {
                    // Throwable, not Exception: an assertion failure here is an AssertionError
                    failure.compareAndSet(null, t);
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        assertThat("Concurrent refresh/read did not complete", executor.awaitTermination(30, TimeUnit.SECONDS), is(true));

        assertThat("Concurrent refresh/read caused failure: " + failure.get(), failure.get(), is(nullValue()));
    }

    @Test
    void findPluginClass_resolves_every_name_published_in_the_multi_valued_name_property() throws Exception {
        final Bundle bundle = bundleLoading(TestPluginType.class, TestPlugin.class);
        final OsgiPluginRegistry mockedRegistry = registryOver(pluginServiceReference(
                bundle,
                new String[] {"test-plugin", "test_plugin", "alternate-plugin"},
                TestPluginType.class.getName(),
                TestPlugin.class.getName()));

        assertThat(mockedRegistry.getPluginCount(), is(3));
        assertResolvesToTestPlugin(mockedRegistry, "test-plugin");
        assertResolvesToTestPlugin(mockedRegistry, "test_plugin");
        assertResolvesToTestPlugin(mockedRegistry, "alternate-plugin");
    }

    @Test
    void findPluginClass_resolves_a_plugin_without_getting_any_service_from_the_bundle_context() throws Exception {
        final Bundle bundle = bundleLoading(TestPluginType.class, TestPlugin.class);
        final BundleContext mockedContext = contextOver(pluginServiceReference(
                bundle,
                "test-plugin",
                TestPluginType.class.getName(),
                TestPlugin.class.getName()));
        final OsgiPluginRegistry mockedRegistry = new OsgiPluginRegistry(mockedContext);

        assertResolvesToTestPlugin(mockedRegistry, "test-plugin");

        // Discovery reads service properties and resolves the class through the publishing bundle.
        // Calling getService() would activate every published plugin service as a side effect of a
        // lookup, so a regression that reintroduces it must fail here.
        verify(mockedContext, never()).getService(any());
    }

    @Test
    void findPluginClass_resolves_a_name_published_as_a_plain_string_property() throws Exception {
        final Bundle bundle = bundleLoading(TestPluginType.class, TestPlugin.class);
        final OsgiPluginRegistry mockedRegistry = registryOver(pluginServiceReference(
                bundle,
                "test-plugin",
                TestPluginType.class.getName(),
                TestPlugin.class.getName()));

        assertThat(mockedRegistry.getPluginCount(), is(1));
        assertResolvesToTestPlugin(mockedRegistry, "test-plugin");
    }

    @Test
    void scanServices_skips_a_service_missing_the_plugin_class_property() throws Exception {
        final Bundle bundle = bundleLoading(TestPluginType.class, TestPlugin.class);
        final OsgiPluginRegistry mockedRegistry = registryOver(pluginServiceReference(
                bundle,
                "test-plugin",
                TestPluginType.class.getName(),
                null));

        assertThat(mockedRegistry.getPluginCount(), is(0));
        assertThat(mockedRegistry.findPluginClass(TestPluginType.class, "test-plugin").isPresent(), is(false));
    }

    @Test
    void scanServices_skips_a_service_missing_the_plugin_type_property() throws Exception {
        final Bundle bundle = bundleLoading(TestPluginType.class, TestPlugin.class);
        final OsgiPluginRegistry mockedRegistry = registryOver(pluginServiceReference(
                bundle,
                "test-plugin",
                null,
                TestPlugin.class.getName()));

        assertThat(mockedRegistry.getPluginCount(), is(0));
    }

    @Test
    void scanServices_skips_a_service_whose_plugin_class_cannot_be_loaded() throws Exception {
        final Bundle bundle = bundleLoading(TestPluginType.class);
        doThrow(new ClassNotFoundException("missing"))
                .when(bundle).loadClass(TestPlugin.class.getName());
        final OsgiPluginRegistry mockedRegistry = registryOver(pluginServiceReference(
                bundle,
                "test-plugin",
                TestPluginType.class.getName(),
                TestPlugin.class.getName()));

        assertThat(mockedRegistry.getPluginCount(), is(0));
        assertThat(mockedRegistry.findPluginClass(TestPluginType.class, "test-plugin").isPresent(), is(false));
    }

    @Test
    void scanServices_with_null_service_references_produces_an_empty_registry() throws Exception {
        final BundleContext mockedContext = mock(BundleContext.class);
        doReturn(null).when(mockedContext).getAllServiceReferences(Class.class.getName(), PLUGIN_SERVICE_FILTER);
        final OsgiPluginRegistry mockedRegistry = new OsgiPluginRegistry(mockedContext);

        assertThat(mockedRegistry.scanServices().isEmpty(), is(true));
        assertThat(mockedRegistry.getPluginCount(), is(0));
    }

    private static void assertResolvesToTestPlugin(final OsgiPluginRegistry pluginRegistry, final String pluginName) {
        final Optional<Class<? extends TestPluginType>> expected = Optional.of(TestPlugin.class);
        assertThat("Plugin name '" + pluginName + "' did not resolve to " + TestPlugin.class.getSimpleName(),
                pluginRegistry.findPluginClass(TestPluginType.class, pluginName), is(expected));
    }

    private static OsgiPluginRegistry registryOver(final ServiceReference<?>... references) throws Exception {
        return new OsgiPluginRegistry(contextOver(references));
    }

    private static BundleContext contextOver(final ServiceReference<?>... references) throws Exception {
        final BundleContext mockedContext = mock(BundleContext.class);
        doReturn(references)
                .when(mockedContext).getAllServiceReferences(Class.class.getName(), PLUGIN_SERVICE_FILTER);
        return mockedContext;
    }

    private static ServiceReference<?> pluginServiceReference(final Bundle bundle,
                                                             final Object nameProperty,
                                                             final String pluginTypeName,
                                                             final String pluginClassName) {
        final ServiceReference<?> reference = mock(ServiceReference.class);
        doReturn(bundle).when(reference).getBundle();
        doReturn(nameProperty).when(reference).getProperty(OsgiPluginRegistry.PLUGIN_NAME_PROPERTY);
        doReturn(pluginTypeName).when(reference).getProperty(OsgiPluginRegistry.PLUGIN_TYPE_PROPERTY);
        doReturn(pluginClassName).when(reference).getProperty(OsgiPluginRegistry.PLUGIN_CLASS_PROPERTY);
        return reference;
    }

    /**
     * Creates a bundle that can load the given classes by name. The bundle is a mock, so
     * {@code adapt(BundleWiring.class)} returns null and {@link BundleClassLoaderScope} leaves the
     * thread context classloader untouched, which the scope tolerates.
     *
     * @param loadableClasses the classes the bundle can load
     * @return the mocked bundle
     */
    private static Bundle bundleLoading(final Class<?>... loadableClasses) throws Exception {
        final Bundle bundle = mock(Bundle.class);
        for (final Class<?> loadableClass : loadableClasses) {
            doReturn(loadableClass).when(bundle).loadClass(loadableClass.getName());
        }
        return bundle;
    }

    interface TestPluginType {
    }

    static class TestPlugin implements TestPluginType {
    }
}
