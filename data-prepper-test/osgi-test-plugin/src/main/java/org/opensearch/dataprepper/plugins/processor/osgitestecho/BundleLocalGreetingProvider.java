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
 * The only implementation of {@link OsgiTestGreetingProvider}, declared in this bundle's
 * {@code META-INF/services} directory and packaged inside the bundle JAR.
 */
public class BundleLocalGreetingProvider implements OsgiTestGreetingProvider {
    /**
     * The value the OSGi integration test asserts on. It is deliberately distinct from
     * {@link OsgiTestEchoProcessor#SPI_UNAVAILABLE_GREETING} so that a failed SPI lookup is visible
     * rather than silently indistinguishable from a successful one.
     */
    static final String GREETING = "greeting-from-bundle-spi";

    @Override
    public String greeting() {
        return GREETING;
    }
}
