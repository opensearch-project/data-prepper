package org.opensearch.dataprepper.schemas.module;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.github.victools.jsonschema.generator.MemberScope;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jackson.JacksonOption;

public class CustomJacksonModule extends JacksonModule {

    public CustomJacksonModule() {
        super();
    }

    public CustomJacksonModule(JacksonOption... options) {
        super(options);
    }

    @Override
    protected String getPropertyNameOverrideBasedOnJsonPropertyAnnotation(MemberScope<?, ?> member) {
        JsonProperty annotation = member.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
        if (annotation != null) {
            String nameOverride = annotation.value();
            // check for invalid overrides
            if (nameOverride != null && !nameOverride.isEmpty() && !nameOverride.equals(member.getDeclaredName())) {
                return nameOverride;
            }
        }
        return PropertyNamingStrategies.SNAKE_CASE.nameForField(null, null, member.getName());
    }
}
