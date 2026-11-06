/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin;

/**
 * A component which registers {@link PluginProvider}s with the {@link PluginProviderLoader} during
 * its own initialization rather than through the classpath {@link java.util.ServiceLoader}.
 * <p>
 * {@link DefaultPluginFactory} declares a dependency on this type so that Spring must fully
 * initialize the registrar — including its {@code @PostConstruct} — before it constructs the plugin
 * factory. Without that edge, the first plugin lookup can race the registration and fail
 * nondeterministically depending on bean instantiation order.
 * <p>
 * The dependency is declared as an {@link java.util.Optional} because the only implementation lives
 * in the {@code plugin-framework-osgi} module, which {@code data-prepper-core} depends on at runtime
 * only. Contexts that do not include that module, including many unit and integration test contexts,
 * simply observe an empty {@code Optional} and are unaffected.
 *
 * @since 2.17
 */
public interface PluginProviderRegistrar {
    /**
     * Returns whether this registrar has finished registering every provider it intends to register.
     * This is reported in the diagnostic Data Prepper emits when no plugin provider is available, so
     * that a genuine registration failure is not misreported as a startup ordering problem.
     *
     * @return true once registration has completed, whether or not any provider was registered
     */
    boolean isPluginProviderRegistrationComplete();
}
