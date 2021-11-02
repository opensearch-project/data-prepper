package com.amazon.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Add to a plugin class to indicate which constructor should be used by Data Prepper.
 * <p>
 * The current behavior for choosing a constructor is:
 * <ol>
 *     <li>Choose the constructor annotated with {@link DataPrepperPluginConstructor}</li>
 *     <li>Choose a constructor which takes in a single parameter matching
 *     the {@link DataPrepperPlugin#pluginConfigurationType()} for the plugin</li>
 *     <li>Use the default (ie. empty) constructor</li>
 * </ol>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.CONSTRUCTOR})
public @interface DataPrepperPluginConstructor {
}
