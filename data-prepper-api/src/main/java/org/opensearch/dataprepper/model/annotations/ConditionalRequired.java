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
     * @return returns array of if and else values
     */
    IfThenElse[] value();

    /**
     * Annotation to represent an if-then-else requirement.
     */
    @interface IfThenElse {
        /**
         * Array of property schemas involved in if condition.
         * @return returns of if schema properties
         */
        SchemaProperty[] ifFulfilled();
        /**
         * Array of property schemas involved in then expectation.
         * @return returns of then schema properties
         */
        SchemaProperty[] thenExpect();
        /**
         * Array of property schemas involved in else expectation.
         * @return returns of else schema properties
         */
        SchemaProperty[] elseExpect() default {};
    }

    /**
     * Annotation to represent a property schema.
     */
    @interface SchemaProperty {
        /**
         * Name of the property.
         * @return returns schema field
         */
        String field();
        /**
         * Value of the property. Empty string means any non-null value is allowed.
         * @return returns schema value
         */
        String value() default "";
    }
}
