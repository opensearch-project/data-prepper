package org.opensearch.dataprepper.model.annotations;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Marker interface to indicate that a Data Prepper plugin is interested in using Dependency Injection.
 * If a plugin implements this interface, then the core Data Prepper initialize the SpringContext on the
 * plugin related classes and makes the context available to use by the setter method.
 *
 * Read more here at PluginBeanFactoryProvide
 */
public interface PluginDiContextAware {

    void setPluginDIContext(AnnotationConfigApplicationContext pluginDIContext);
}
