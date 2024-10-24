package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used in schema generation to define the if-then-else requirements.
 */
@Target({ ElementType.FIELD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface ConditionalRequired {
    /**
     * Array of if-then-else requirements.
     */
    IfThenElse[] value();

    /**
     * Annotation to represent an if-then-else requirement.
     */
    @interface IfThenElse {
        /**
         * Array of property schemas involved in if condition.
         */
        SchemaProperty[] ifFulfilled();
        /**
         * Array of property schemas involved in then expectation.
         */
        SchemaProperty[] thenExpect();
        /**
         * Array of property schemas involved in else expectation.
         */
        SchemaProperty[] elseExpect() default {};
    }

    /**
     * Annotation to represent a property schema.
     */
    @interface SchemaProperty {
        /**
         * Name of the property.
         */
        String field();
        /**
         * Value of the property. Empty string means any non-null value is allowed.
         */
        String value() default "";
    }
}
