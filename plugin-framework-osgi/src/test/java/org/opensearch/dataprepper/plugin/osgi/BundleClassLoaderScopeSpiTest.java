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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ServiceLoader;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Proves that {@link BundleClassLoaderScope} enables SPI resolution via
 * {@link ServiceLoader#load(Class)} when the TCCL is set to a classloader
 * that contains the META-INF/services registration.
 * <p>
 * This test creates a self-contained "bundle" JAR on disk that:
 * <ol>
 *   <li>Contains a service interface ({@link SpiTestService})</li>
 *   <li>Contains an implementation class</li>
 *   <li>Registers the implementation in META-INF/services</li>
 * </ol>
 * It then demonstrates that:
 * <ul>
 *   <li>WITHOUT the scope (TCCL = test classloader), ServiceLoader does NOT find
 *       the bundle's implementation</li>
 *   <li>WITH the scope (TCCL = bundle URLClassLoader), ServiceLoader DOES find it</li>
 * </ul>
 */
class BundleClassLoaderScopeSpiTest {

    @TempDir
    File tempDir;

    private ClassLoader originalTccl;

    @BeforeEach
    void setUp() {
        originalTccl = Thread.currentThread().getContextClassLoader();
    }

    @AfterEach
    void tearDown() {
        Thread.currentThread().setContextClassLoader(originalTccl);
    }

    /**
     * A minimal SPI interface used only by this test. Intentionally kept
     * as an inner interface so it is available on the test classpath.
     */
    public interface SpiTestService {
        String name();
    }

    @Test
    void serviceLoader_without_scope_does_not_find_bundle_impl() throws Exception {
        final URLClassLoader bundleClassLoader = createBundleClassLoaderWithSpi();

        // Without the scope, TCCL is the test runner's classloader.
        // ServiceLoader.load uses TCCL, which does NOT have the bundle's META-INF/services.
        final ServiceLoader<SpiTestService> loader = ServiceLoader.load(SpiTestService.class);
        final long count = loader.stream().count();

        assertThat("Without TCCL management, the bundle SPI impl should NOT be found", count, is(0L));

        bundleClassLoader.close();
    }

    @Test
    void serviceLoader_with_scope_finds_bundle_impl() throws Exception {
        final URLClassLoader bundleClassLoader = createBundleClassLoaderWithSpi();

        // With the scope, TCCL is set to the bundle classloader that HAS META-INF/services.
        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundleClassLoader)) {
            final ServiceLoader<SpiTestService> loader = ServiceLoader.load(SpiTestService.class);
            final long count = loader.stream().count();

            assertThat("With TCCL managed, the bundle SPI impl SHOULD be found", count, is(1L));

            // Verify the loaded service is functional
            final SpiTestService service = loader.iterator().next();
            assertThat(service.name(), is("bundle-spi-impl"));
        }

        bundleClassLoader.close();
    }

    @Test
    void tccl_is_restored_after_spi_resolution() throws Exception {
        final URLClassLoader bundleClassLoader = createBundleClassLoaderWithSpi();

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundleClassLoader)) {
            ServiceLoader.load(SpiTestService.class);
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(originalTccl));

        bundleClassLoader.close();
    }

    /**
     * Creates a URLClassLoader that simulates a bundle classloader.
     * The classloader's URL points to a directory structure containing:
     * - The compiled SpiTestService interface (from the test classpath)
     * - A compiled implementation class
     * - META-INF/services/[interface-name] pointing to the impl
     *
     * We use a directory-based classloader for simplicity (no need to compile
     * at test time). The implementation is a simple class loaded from the same
     * test classpath but wrapped in a child classloader with the services file.
     */
    private URLClassLoader createBundleClassLoaderWithSpi() throws IOException {
        // Create a directory that acts as a classpath entry with META-INF/services
        final File bundleDir = new File(tempDir, "bundle-classes");
        bundleDir.mkdirs();

        // Write the META-INF/services descriptor pointing to the impl class.
        // The impl class (BundleSpiTestServiceImpl) is on the test classpath, so
        // a child classloader that also has bundleDir on its path will find both
        // the interface (from parent) and the services file (from bundleDir).
        final File metaInf = new File(bundleDir, "META-INF/services");
        metaInf.mkdirs();
        final File servicesFile = new File(metaInf,
                BundleClassLoaderScopeSpiTest.SpiTestService.class.getName());
        java.nio.file.Files.writeString(servicesFile.toPath(),
                BundleSpiTestServiceImpl.class.getName() + "\n");

        // Create a URLClassLoader whose classpath includes bundleDir (for META-INF/services)
        // and whose parent is the test classloader (for the interface and impl classes).
        // This mirrors how a real OSGi bundle classloader would expose both its own
        // META-INF/services and the classes visible through wiring.
        return new URLClassLoader(
                new URL[]{bundleDir.toURI().toURL()},
                this.getClass().getClassLoader()
        );
    }

    /**
     * A trivial SPI implementation that lives on the test classpath.
     * The META-INF/services file in the "bundle" directory references this class.
     */
    public static final class BundleSpiTestServiceImpl implements SpiTestService {
        public BundleSpiTestServiceImpl() {
            // Required public no-arg constructor for ServiceLoader
        }

        @Override
        public String name() {
            return "bundle-spi-impl";
        }
    }
}
