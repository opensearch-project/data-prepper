/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.schemas.module;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.victools.jsonschema.generator.MemberScope;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;

/**
 * Custom {@link JakartaValidationModule} which overrides the default behavior of {@link JakartaValidationModule}
 * for Data Prepper.
 * It considers the {@link JsonProperty} annotation as well as the Jakarta validations.
 */
class DataPrepperJakartaValidationModule extends JakartaValidationModule {
    public DataPrepperJakartaValidationModule(final JakartaValidationOption... options) {
        super(options);
    }

    @Override
    protected boolean isRequired(final MemberScope<?, ?> member) {
        final JsonProperty jsonPropertyAnnotation = member.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
        if(jsonPropertyAnnotation != null) {
            if(jsonPropertyAnnotation.defaultValue() != null && !jsonPropertyAnnotation.defaultValue().isEmpty()) {
                return false;
            }
        }

        return super.isRequired(member);
    }
}
