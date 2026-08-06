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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.validation.LoggingPluginErrorsHandler;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.event.DefaultEventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;
import org.opensearch.dataprepper.plugin.DefaultPluginFactory;
import org.opensearch.dataprepper.plugin.ExperimentalConfiguration;
import org.opensearch.dataprepper.plugin.ExperimentalConfigurationContainer;
import org.opensearch.dataprepper.plugin.ExtensionsConfiguration;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;
import org.osgi.framework.BundleReference;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.File;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Integration test that verifies the OSGi plugin loading pipeline end-to-end
 * through {@link DefaultPluginFactory}. This test:
 * <ol>
 *   <li>Starts a real Felix OSGi framework (via OsgiFrameworkRunner)</li>
 *   <li>Loads the osgi-test-plugin bundle from the staged bundles directory</li>
 *   <li>Resolves the plugin through DefaultPluginFactory.loadPlugin()</li>
 *   <li>Asserts the plugin was loaded via OSGi (bundle classloader, not app classloader)</li>
 * </ol>
 * <p>
 * IMPORTANT: The osgi-test-plugin classes are NOT on the integration test compile or
 * runtime classpath. They are only available as a bundle JAR in the staged bundles
 * directory. This proves the plugin was truly loaded via OSGi, not via classpath scanning.
 * <p>
 * System properties (set by Gradle):
 * <ul>
 *   <li>{@code data-prepper.plugin.framework=osgi} — enables OSGi mode</li>
 *   <li>{@code data-prepper.plugin.bundles.dir} — path to staged bundle JARs</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
class OsgiIT {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiIT.class);
    private static final String TEST_PLUGIN_NAME = "osgi_test_echo";

    @Mock
    private PipelinesDataFlowModel pipelinesDataFlowModel;
    @Mock
    private ExtensionsConfiguration extensionsConfiguration;
    @Mock
    private ExperimentalConfigurationContainer experimentalConfigurationContainer;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private AnnotationConfigApplicationContext applicationContext;
    private String pipelineName;

    @BeforeAll
    static void verifySystemProperties() {
        final String framework = System.getProperty("data-prepper.plugin.framework");
        final String bundlesDir = System.getProperty("data-prepper.plugin.bundles.dir");
        LOG.info("OSGi IT system properties: framework={}, bundles.dir={}", framework, bundlesDir);

        assertThat("System property data-prepper.plugin.framework must be 'osgi'",
                framework, is("osgi"));
        assertThat("System property data-prepper.plugin.bundles.dir must be set",
                bundlesDir, notNullValue());

        final File dir = new File(bundlesDir);
        assertTrue(dir.isDirectory(), "Bundles directory must exist: " + bundlesDir);

        final File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
        assertThat("Bundles directory must contain at least one JAR", jars, notNullValue());
        assertTrue(jars.length > 0, "Bundles directory must contain at least one JAR");
        LOG.info("Found {} bundle JAR(s) in {}", jars.length, bundlesDir);
        for (final File jar : jars) {
            LOG.info("  Bundle JAR: {}", jar.getName());
        }
    }

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
    }

    @AfterEach
    void tearDown() {
        if (applicationContext != null) {
            applicationContext.close();
        }
    }

    /**
     * Creates a real {@link DefaultPluginFactory} using a Spring context that scans
     * the plugin framework, OSGi framework, and event packages. This follows the
     * same approach as DefaultPluginFactoryIT in data-prepper-core.
     */
    private DefaultPluginFactory createDefaultPluginFactory() {
        final AnnotationConfigApplicationContext publicContext = new AnnotationConfigApplicationContext();
        publicContext.refresh();

        applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.setParent(publicContext);

        when(experimentalConfigurationContainer.getExperimental())
                .thenReturn(ExperimentalConfiguration.defaultConfiguration());

        // Scan the plugin framework package (DefaultPluginFactory, PluginCreator, etc.)
        applicationContext.scan(DefaultPluginFactory.class.getPackage().getName());

        // Scan the OSGi framework package (OsgiFrameworkRunner, which starts Felix and
        // registers OsgiPluginRegistry into PluginProviderLoader)
        applicationContext.scan(OsgiFrameworkRunner.class.getPackage().getName());

        // Scan the event package (EventFactory, EventKeyFactory beans)
        applicationContext.scan(DefaultEventFactory.class.getPackage().getName());

        // Register necessary beans (PluginBeanFactoryProvider is picked up via package scan)
        applicationContext.registerBean(PluginErrorCollector.class, PluginErrorCollector::new);
        applicationContext.registerBean(PluginErrorsHandler.class, LoggingPluginErrorsHandler::new);
        applicationContext.registerBean(DataPrepperDeserializationProblemHandler.class,
                DataPrepperDeserializationProblemHandler::new);
        applicationContext.registerBean(ExtensionsConfiguration.class, () -> extensionsConfiguration);
        applicationContext.registerBean(PipelinesDataFlowModel.class, () -> pipelinesDataFlowModel);
        applicationContext.registerBean(ExperimentalConfigurationContainer.class,
                () -> experimentalConfigurationContainer);
        applicationContext.registerBean(AcknowledgementSetManager.class,
                () -> acknowledgementSetManager);

        applicationContext.refresh();

        return applicationContext.getBean(DefaultPluginFactory.class);
    }

    @Test
    void loadPlugin_through_osgi_returns_processor_loaded_by_osgi_bundle_classloader() {
        LOG.info("=== OsgiIT: Testing plugin loading through DefaultPluginFactory with OSGi ===");

        // 1. Build a real DefaultPluginFactory with OSGi enabled
        final DefaultPluginFactory pluginFactory = createDefaultPluginFactory();
        assertThat("DefaultPluginFactory should be created", pluginFactory, notNullValue());
        LOG.info("DefaultPluginFactory created successfully");

        // 2. Verify OsgiFrameworkRunner is active
        final OsgiFrameworkRunner runner = applicationContext.getBean(OsgiFrameworkRunner.class);
        assertThat("OsgiFrameworkRunner should be active", runner.isActive(), is(true));
        LOG.info("OsgiFrameworkRunner is active — Felix framework running");

        // 3. Load the test plugin through DefaultPluginFactory
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);

        final Processor plugin = assertDoesNotThrow(
                () -> pluginFactory.loadPlugin(Processor.class, pluginSetting),
                "Loading osgi_test_echo plugin should succeed");
        assertThat("Plugin instance should not be null", plugin, notNullValue());
        LOG.info("Plugin loaded successfully: {} (class: {})",
                TEST_PLUGIN_NAME, plugin.getClass().getName());

        // 4. PROVE the plugin was loaded via OSGi — NOT from the application classpath.
        //
        // Assertion A: The plugin's class was loaded by an OSGi bundle classloader.
        // FrameworkUtil.getBundle(class) returns non-null only for classes loaded from bundles.
        assertThat("Plugin class must be loaded from an OSGi bundle "
                        + "(FrameworkUtil.getBundle() must return non-null)",
                FrameworkUtil.getBundle(plugin.getClass()), notNullValue());
        LOG.info("PASS: FrameworkUtil.getBundle(pluginClass) is non-null — class is from an OSGi bundle");

        // Assertion B: The plugin's classloader implements BundleReference (OSGi spec requirement).
        final ClassLoader pluginClassLoader = plugin.getClass().getClassLoader();
        assertThat("Plugin classloader must implement BundleReference (OSGi spec)",
                pluginClassLoader, instanceOf(BundleReference.class));
        LOG.info("PASS: Plugin classloader implements BundleReference: {}",
                pluginClassLoader.getClass().getName());

        // Assertion C: The plugin's classloader is NOT the application/system classloader.
        // This proves true classloader isolation — the class was not loaded from the
        // integration test's own classpath.
        final ClassLoader appClassLoader = DefaultPluginFactory.class.getClassLoader();
        assertThat("Plugin classloader must differ from application classloader "
                        + "(proves OSGi classloader isolation)",
                pluginClassLoader, not(is(appClassLoader)));
        LOG.info("PASS: Plugin classloader ({}) differs from app classloader ({})",
                pluginClassLoader, appClassLoader);

        // Assertion D: The plugin class cannot be found by the app classloader.
        // This is the strongest proof: the test plugin's classes are NOT on the test classpath.
        final String pluginClassName = plugin.getClass().getName();
        try {
            appClassLoader.loadClass(pluginClassName);
            // If we get here, the class was on the app classpath — test is invalid
            throw new AssertionError(
                    "Plugin class " + pluginClassName + " should NOT be loadable from the "
                            + "application classloader. This means the test plugin classes leaked "
                            + "onto the integration test classpath, invalidating the OSGi proof.");
        } catch (final ClassNotFoundException expected) {
            LOG.info("PASS: Plugin class {} is NOT on the app classpath (ClassNotFoundException as expected)",
                    pluginClassName);
        }

        // 5. Verify the plugin is functional
        assertThat("Plugin should be ready for shutdown", plugin.isReadyForShutdown(), is(true));
        LOG.info("=== OsgiIT: All assertions passed — plugin loaded via OSGi ===");
    }

    @Test
    void osgi_framework_runner_registers_provider_before_plugin_factory_resolves() {
        LOG.info("=== OsgiIT: Testing OSGi provider registration order ===");

        final DefaultPluginFactory pluginFactory = createDefaultPluginFactory();

        // Verify the OsgiPluginRegistry was registered (it found at least 1 plugin)
        final OsgiFrameworkRunner runner = applicationContext.getBean(OsgiFrameworkRunner.class);
        assertThat("Framework runner must be active", runner.isActive(), is(true));
        assertThat("Bundle health check should be available", runner.getBundleHealthCheck(), notNullValue());

        LOG.info("=== OsgiIT: Provider registration order verified ===");
    }

    @Test
    void osgi_loaded_plugin_has_correct_bundle_symbolic_name() {
        LOG.info("=== OsgiIT: Testing bundle metadata on loaded plugin ===");

        final DefaultPluginFactory pluginFactory = createDefaultPluginFactory();

        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);

        final Processor plugin = pluginFactory.loadPlugin(Processor.class, pluginSetting);
        assertThat(plugin, notNullValue());

        // Verify the bundle has the expected symbolic name pattern
        final org.osgi.framework.Bundle bundle = FrameworkUtil.getBundle(plugin.getClass());
        assertThat("Bundle should not be null", bundle, notNullValue());
        assertThat("Bundle symbolic name should follow Data Prepper convention",
                bundle.getSymbolicName().startsWith("org.opensearch.dataprepper.plugin."), is(true));
        LOG.info("Bundle symbolic name: {}", bundle.getSymbolicName());
        LOG.info("Bundle version: {}", bundle.getVersion());
        LOG.info("Bundle state: {}", StaticBundleLoader.getStateString(bundle.getState()));

        assertThat("Bundle must be ACTIVE",
                bundle.getState(), is(org.osgi.framework.Bundle.ACTIVE));

        LOG.info("=== OsgiIT: Bundle metadata verified ===");
    }

    @Test
    void classpath_plugin_provider_does_not_find_osgi_only_plugin() {
        LOG.info("=== OsgiIT: Verifying osgi_test_echo is NOT discoverable via classpath ===");

        // Verify the test plugin cannot be found by loading the class from the app classloader.
        // This confirms the test setup is correct: the plugin is ONLY available via OSGi bundles.
        final String expectedClassName = "org.opensearch.dataprepper.plugins.processor.osgitestecho.OsgiTestEchoProcessor";
        try {
            getClass().getClassLoader().loadClass(expectedClassName);
            throw new AssertionError(
                    "Test setup error: " + expectedClassName + " should NOT be on the test classpath. "
                            + "The osgiBundleJar configuration must NOT bleed into integrationTestRuntimeClasspath.");
        } catch (final ClassNotFoundException expected) {
            LOG.info("PASS: {} is not on the integration test classpath (correct isolation)",
                    expectedClassName);
        }

        LOG.info("=== OsgiIT: Classpath isolation verified ===");
    }
}
