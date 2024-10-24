package org.opensearch.dataprepper.model.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface ConditionalRequired {
    IfThenElse[] value();

    @interface IfThenElse {
        SchemaProperty[] ifFulfilled();
        SchemaProperty[] thenExpect();
        SchemaProperty[] elseExpect() default {};
    }

    @interface SchemaProperty {
        String field();
        String value() default "";
    }
}
