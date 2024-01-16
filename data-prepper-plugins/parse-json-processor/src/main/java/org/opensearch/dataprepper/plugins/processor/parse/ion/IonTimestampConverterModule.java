/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import com.amazon.ion.Timestamp;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class IonTimestampConverterModule extends SimpleModule {
    public IonTimestampConverterModule() {
        addSerializer(Timestamp.class, new StdSerializer<>(Timestamp.class) {
            @Override
            public void serialize(Timestamp value, JsonGenerator gen, SerializerProvider provider) throws IOException {
                gen.writeString(value.toZString());
            }
        });
    }
}
