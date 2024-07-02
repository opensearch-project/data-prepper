package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a Data Prepper extension plugin which includes a configuration model class.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface DataPrepperExtensionPlugin {
    String DEFAULT_DEPRECATED_ROOT_KEY_JSON_PATH = "";

    /**
     * @return extension plugin configuration class.
     */
    Class<?> modelType();

    /**
     * @return valid JSON path string starts with "/" pointing towards the configuration block.
     */
    String deprecatedRootKeyJsonPath() default DEFAULT_DEPRECATED_ROOT_KEY_JSON_PATH;

    /**
     * @return valid JSON path string starts with "/" pointing towards the configuration block.
     */
    String rootKeyJsonPath();

    boolean allowInPipelineConfigurations() default false;
}
