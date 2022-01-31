/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.event;

import com.fasterxml.jackson.datatype.jsr310.ser.ZonedDateTimeSerializer;

import java.time.format.DateTimeFormatterBuilder;

/**
 * A Custom Serializer for ZonedDateTime which formats value of {@link JacksonEvent#TIMESTAMP_KEY}
 * @since 1.3
 */
public class CustomZonedDateTimeSerializerWithMilliSecondPrecision extends ZonedDateTimeSerializer {
    public CustomZonedDateTimeSerializerWithMilliSecondPrecision() {
        super(new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                        .toFormatter());
    }
}
