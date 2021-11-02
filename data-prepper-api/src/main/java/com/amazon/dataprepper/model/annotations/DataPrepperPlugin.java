/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model.annotations;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotates a Data Prepper plugin. This can be a pipeline component such
 * as {@link com.amazon.dataprepper.model.source.Source},
 * {@link com.amazon.dataprepper.model.buffer.Buffer},
 * {@link com.amazon.dataprepper.model.prepper.Prepper}, or
 * {@link com.amazon.dataprepper.model.sink.Sink}. It can also be another
 * plugin-supported class.
 * <p>
 * The value provided in the {@link #name()} attribute determines the name of
 * the plugin as would be found in the pipeline configuration.
 * <p>
 * You must define either the {@link #pluginType()} or {@link #type()} to load
 * as a plugin. Right now, neither are required at compile-time. However, in a
 * future release of Data Prepper we will make {@link #pluginType()} required
 * and remove {@link #type()} altogether.
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
     * The enum of supported pipeline component types.
     *
     * @deprecated Remove in favor of {@link DataPrepperPlugin#pluginType()}.
     * This value will be removed in an upcoming release of Data Prepper.
     * @return The plugin type enum
     */
    @Deprecated
    PluginType type() default PluginType.NONE;

    /**
     * The class type for this plugin.
     * <p>
     * While this property is not currently required, you should supply it. A
     * future version of Data Prepper will make this a required attribute.
     *
     * @return The Java class
     * @since 1.2
     */
    Class<?> pluginType() default Void.class;

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
