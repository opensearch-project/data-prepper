package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used in schema generation to define the names and corresponding values of other required
 * properties to be returned, if the property represented by the annotated field/method is present.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface IfPresentAlsoRequire {
    /**
     * Array of strings in which each string should represent property(:value) where :value can be optional.
     */
    String[] values();
}
