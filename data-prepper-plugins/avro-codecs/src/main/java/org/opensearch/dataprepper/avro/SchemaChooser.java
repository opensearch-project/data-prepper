/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

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
