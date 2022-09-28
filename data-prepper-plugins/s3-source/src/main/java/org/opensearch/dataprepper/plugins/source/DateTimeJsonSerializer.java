/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import java.io.IOException;
import java.util.Date;

import software.amazon.awssdk.utils.DateUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * A Jackson serializer for Joda {@code DateTime}s.
 */
final class DateTimeJsonSerializer extends JsonSerializer<Date> {

  @Override
  public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
    jgen.writeString(DateUtils.formatIso8601Date(value.toInstant()));
  }

}