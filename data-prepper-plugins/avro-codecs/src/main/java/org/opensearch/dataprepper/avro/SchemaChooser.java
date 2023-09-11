package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;

class SchemaChooser {
    Schema chooseSchema(Schema providedSchema) {
        if(providedSchema.isNullable()) {
            return getFirstNonNullable(providedSchema);
        }

        return providedSchema;
    }

    private Schema getFirstNonNullable(Schema providedSchema) {
        return providedSchema.getTypes()
                .stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(providedSchema);
    }
}
