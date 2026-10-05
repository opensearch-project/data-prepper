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
import org.osgi.framework.wiring.BundleWiring;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BundleClassLoaderScopeTest {

    private ClassLoader originalTccl;

    @BeforeEach
    void setUp() {
        originalTccl = Thread.currentThread().getContextClassLoader();
    }

    @AfterEach
    void tearDown() {
        // Safety net: always restore TCCL after each test
        Thread.currentThread().setContextClassLoader(originalTccl);
    }

    @Test
    void tccl_is_set_within_scope() {
        final ClassLoader bundleCl = new ClassLoader(null) {};

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundleCl)) {
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(bundleCl)));
        }
    }

    @Test
    void tccl_is_restored_after_close() {
        final ClassLoader bundleCl = new ClassLoader(null) {};

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundleCl)) {
            // inside scope — TCCL should be bundleCl
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void tccl_is_restored_even_if_body_throws() {
        final ClassLoader bundleCl = new ClassLoader(null) {};

        try {
            try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundleCl)) {
                assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(bundleCl)));
                throw new RuntimeException("simulated failure");
            }
        } catch (final RuntimeException ignored) {
            // expected
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void nested_scopes_restore_correctly() {
        final ClassLoader outerCl = new ClassLoader(null) {};
        final ClassLoader innerCl = new ClassLoader(null) {};

        try (BundleClassLoaderScope outer = BundleClassLoaderScope.of(outerCl)) {
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(outerCl)));

            try (BundleClassLoaderScope inner = BundleClassLoaderScope.of(innerCl)) {
                assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(innerCl)));
            }

            // After inner close, should be back to outer's classloader
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(outerCl)));
        }

        // After outer close, should be back to original
        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void null_classloader_leaves_tccl_unchanged_but_restores_on_close() {
        final ClassLoader customCl = new ClassLoader(null) {};
        Thread.currentThread().setContextClassLoader(customCl);

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of((ClassLoader) null)) {
            // TCCL should remain customCl since we passed null
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(customCl)));
        }

        // After close, still restored to customCl
        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(customCl)));
    }

    @Test
    void of_bundle_with_valid_wiring_sets_tccl() {
        final ClassLoader bundleCl = new ClassLoader(null) {};
        final BundleWiring wiring = mock(BundleWiring.class);
        when(wiring.getClassLoader()).thenReturn(bundleCl);
        final Bundle bundle = mock(Bundle.class);
        when(bundle.adapt(BundleWiring.class)).thenReturn(wiring);

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundle)) {
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(bundleCl)));
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void of_bundle_with_null_wiring_leaves_tccl_unchanged() {
        final Bundle bundle = mock(Bundle.class);
        when(bundle.adapt(BundleWiring.class)).thenReturn(null);

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundle)) {
            // Wiring is null, so TCCL should remain unchanged
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void of_bundle_with_wiring_but_null_classloader_leaves_tccl_unchanged() {
        final BundleWiring wiring = mock(BundleWiring.class);
        when(wiring.getClassLoader()).thenReturn(null);
        final Bundle bundle = mock(Bundle.class);
        when(bundle.adapt(BundleWiring.class)).thenReturn(wiring);

        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundle)) {
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void of_null_bundle_leaves_tccl_unchanged() {
        try (BundleClassLoaderScope scope = BundleClassLoaderScope.of((Bundle) null)) {
            assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
        }

        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }

    @Test
    void close_is_idempotent() {
        final ClassLoader bundleCl = new ClassLoader(null) {};

        final BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundleCl);
        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(bundleCl)));

        scope.close();
        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));

        // Second close should not cause issues
        scope.close();
        assertThat(Thread.currentThread().getContextClassLoader(), is(sameInstance(originalTccl)));
    }
}
