package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface UsesDataPrepperPlugin {
    /**
     * The class type for this plugin.
     *
     * @return The Java class
     * @since 1.2
     */
    Class<?> pluginType();
}
