/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.opensearch.dataprepper.core.validation.LoggingPluginErrorsHandler;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;
import org.opensearch.dataprepper.plugin.DefaultPluginFactory;
import org.opensearch.dataprepper.plugin.ExperimentalConfiguration;
import org.opensearch.dataprepper.plugin.ExperimentalConfigurationContainer;
import org.opensearch.dataprepper.plugin.ExtensionsConfiguration;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestApplicationContextProvider {
    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(TestApplicationContextProvider.class);

    static ApplicationContext get(final ExtensionContext extensionContext) {
        final ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);
        return store.getOrComputeIfAbsent("applicationContext",
                key -> createPluginFactory(), ApplicationContext.class);
    }

    private static ApplicationContext createPluginFactory() {
        final AnnotationConfigApplicationContext publicContext = new AnnotationConfigApplicationContext();
        publicContext.registerBean(EventFactory.class, TestEventFactory::getTestEventFactory);
        publicContext.registerBean(EventKeyFactory.class, TestEventKeyFactory::getTestEventFactory);
        publicContext.scan("org.opensearch.dataprepper.expression");
        publicContext.refresh();

        final AnnotationConfigApplicationContext coreContext = new AnnotationConfigApplicationContext();
        coreContext.setParent(publicContext);

        final ExperimentalConfigurationContainer experimentalConfigurationContainer = mock(ExperimentalConfigurationContainer.class);
        final ExperimentalConfiguration experimentalConfiguration = mock(ExperimentalConfiguration.class);
        when(experimentalConfigurationContainer.getExperimental()).thenReturn(experimentalConfiguration);

        coreContext.scan(DefaultPluginFactory.class.getPackage().getName());

        coreContext.registerBean(DataPrepperDeserializationProblemHandler.class, DataPrepperDeserializationProblemHandler::new);
        coreContext.registerBean(PluginErrorCollector.class, PluginErrorCollector::new);
        coreContext.registerBean(PluginErrorsHandler.class, LoggingPluginErrorsHandler::new);
        coreContext.registerBean(ExtensionsConfiguration.class, () -> mock(ExtensionsConfiguration.class));
        coreContext.registerBean(PipelinesDataFlowModel.class, () -> mock(PipelinesDataFlowModel.class));
        coreContext.registerBean(AcknowledgementSetManager.class, () -> mock(AcknowledgementSetManager.class));
        coreContext.registerBean(ExperimentalConfigurationContainer.class, () -> experimentalConfigurationContainer);
        coreContext.refresh();

        return coreContext;
    }
}
