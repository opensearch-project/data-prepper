/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.annotations;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a Data Prepper plugin. This can be a pipeline component such
 * as {@link com.amazon.dataprepper.model.source.Source},
 * {@link com.amazon.dataprepper.model.buffer.Buffer},
 * {@link com.amazon.dataprepper.model.processor.Processor}, or
 * {@link com.amazon.dataprepper.model.sink.Sink}. It can also be another
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
    /**
     *
     * @return Name of the plugin which should be unique for the type
     */
    String name();

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
