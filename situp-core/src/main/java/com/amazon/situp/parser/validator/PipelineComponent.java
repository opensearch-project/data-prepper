package com.amazon.situp.parser.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;


@Constraint(validatedBy = { PipelineComponentValidator.class })
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
public @interface PipelineComponent {

    String message() default "Invalid Configuration: Plugin definition is invalid, requires a valid name with optional " +
            "settings";
    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    Type type() ;

    enum Type {
        Source,
        Buffer,
        Processor,
        Sink;
    }
}
