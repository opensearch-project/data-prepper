package com.amazon.situp.parser;

import com.amazon.situp.parser.model.PipelineConfiguration;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Set;

public class PipelineConfigurationValidator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);
    /**
     * Validates the provided Pipeline configuration and exits the execution if validation fails
     *
     * @param pipelineConfiguration Pipeline configuration for validation
     */
    public static void validate(final PipelineConfiguration pipelineConfiguration) {
        LOG.debug("Validating pipeline configuration");
        final ValidatorFactory validatorFactory = Validation.byProvider(ApacheValidationProvider.class)
                .configure().buildValidatorFactory();
        final Validator jsrValidator = validatorFactory.getValidator();
        final Set<ConstraintViolation<PipelineConfiguration>> violations = jsrValidator.validate(pipelineConfiguration);
        if (violations.size() > 0) {
            violations.forEach(violation -> LOG.error("Found invalid configuration: {}",violation.getMessage()));
            validatorFactory.close();
            throw new RuntimeException("Found invalid configuration, cannot proceed");
        }
        validatorFactory.close();
    }


}
