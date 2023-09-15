/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import software.amazon.awssdk.utils.DateUtils;

import java.io.IOException;
import java.util.Date;

/**
 * A Jackson serializer for Joda {@code DateTime}s.
 */
final class DateTimeJsonSerializer extends JsonSerializer<Date> {

  @Override
  public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
    jgen.writeString(DateUtils.formatIso8601Date(value.toInstant()));
  }

}