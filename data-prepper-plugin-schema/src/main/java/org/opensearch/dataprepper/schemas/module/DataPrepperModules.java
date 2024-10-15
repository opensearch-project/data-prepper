/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.schemas.module;

import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;

import java.util.List;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.FLATTENED_ENUMS_FROM_JSONVALUE;
import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_ORDER;
import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;

public class DataPrepperModules {
    public static List<Module> dataPrepperModules() {
        return List.of(
                new DataPrepperJakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS),
                new CustomJacksonModule(RESPECT_JSONPROPERTY_REQUIRED, RESPECT_JSONPROPERTY_ORDER, FLATTENED_ENUMS_FROM_JSONVALUE)
        );
    }
}
