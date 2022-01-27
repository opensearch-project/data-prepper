/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.event;

import com.fasterxml.jackson.datatype.jsr310.ser.OffsetDateTimeSerializer;

import java.time.format.DateTimeFormatterBuilder;

public class OffsetSerializerWithMilliSecondPrecision extends OffsetDateTimeSerializer {
    public OffsetSerializerWithMilliSecondPrecision() {
        super(OffsetDateTimeSerializer.INSTANCE, false,
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                        .toFormatter());
    }
}
