package com.amazon.dataprepper.model.annotations;

import com.amazon.dataprepper.model.PluginType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotates a Data Prepper Java plugin that includes Source, Sink, Buffer and Processor.
 * The value returned from {@link #name()} represents the name of the plugin and is used in the pipeline configuration
 * and the optional {@link #type()}
 *
 * TODO 1. Pick a different name - Plugin, Component, Resource conflicts with
 * other most used frameworks and may confuse users
 * TODO 2. Add capability for ElementType.METHOD
 * TODO 3. Add expected RECORD_TYPE for input and expected RECORD_TYPE for output
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

    PluginType type();
}
