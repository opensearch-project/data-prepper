/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.osgitestecho;

/**
 * An SPI contract whose only implementation is declared in this bundle's own
 * {@code META-INF/services} directory. {@link OsgiTestEchoProcessor} resolves it with the
 * one-argument {@code ServiceLoader.load(Class)} idiom, which reads the thread context classloader.
 * <p>
 * Only the bundle classloader can see the service declaration, so the lookup succeeds exclusively
 * when the plugin framework sets the thread context classloader for plugin construction. That makes
 * the OSGi integration test a real proof of SPI support rather than of plugin loading alone.
 */
public interface OsgiTestGreetingProvider {
    /**
     * @return the greeting the processor stamps onto each event it handles
     */
    String greeting();
}
