/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.annotations;

import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a Data Prepper plugin. This can be a pipeline component such
 * as {@link org.opensearch.dataprepper.model.source.Source},
 * {@link org.opensearch.dataprepper.model.buffer.Buffer},
 * {@link org.opensearch.dataprepper.model.processor.Processor}, or
 * {@link org.opensearch.dataprepper.model.sink.Sink}. It can also be another
 * plugin-supported class.
 * <p>
 * The value provided in the {@link #name()} attribute determines the name of
 * the plugin as would be found in the pipeline configuration.
 * <p>
 * You must define the {@link #pluginType()} to load your class as a plugin.
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DataPrepperPlugin {
    String DEFAULT_DEPRECATED_NAME = "";

    /**
     *
     * @return Name of the plugin which should be unique for the type
     */
    String name();

    /**
     *
     * @return Deprecated name of the plugin which should be unique for the type
     * @since 2.2
     */
    String deprecatedName() default DEFAULT_DEPRECATED_NAME;

    /**
     * The class type for this plugin.
     *
     * @return The Java class
     * @since 1.2
     */
    Class<?> pluginType();

    /**
     * The configuration type which the plugin takes in the constructor.
     * <p>
     * By default, this value is a {@link PluginSetting}, but you can provide
     * a POJO object to facilitate cleaner code in your plugins.
     *
     * @return The Java class type for plugin configurations
     * @since 1.2
     */
    Class<?> pluginConfigurationType() default PluginSetting.class;
}
