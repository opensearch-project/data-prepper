/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

class ProtobufMessageConverterTest {
    private final ProtobufMessageConverter objectUnderTest = new ProtobufMessageConverter(new ObjectMapper());

    @Test
    void convert_withScalarWrapper_returnsScalarValue() throws Exception {
        assertThat(objectUnderTest.convert(StringValue.of("test-value")), equalTo("test-value"));
    }

    @Test
    void convert_withSixtyFourBitWrappers_returnsNumbers() throws Exception {
        assertThat(objectUnderTest.convert(Int64Value.of(-12L)), equalTo(-12L));
        assertThat(objectUnderTest.convert(UInt64Value.of(12L)), equalTo(12L));
        assertThat(objectUnderTest.convert(UInt64Value.of(-1L)), equalTo(new BigInteger("18446744073709551615")));
    }

    @Test
    void convert_withSixtyFourBitIntegerFields_returnsNumbers() throws Exception {
        final Object convertedValue = objectUnderTest.convert(createIntegerMessage());

        assertThat(convertedValue, instanceOf(Map.class));
        final Map<String, Object> convertedMap = (Map<String, Object>) convertedValue;
        assertThat(convertedMap.get("count"), equalTo(12L));
        assertThat(convertedMap.get("signedFixed"), equalTo(-5L));
        assertThat(convertedMap.get("unsignedMax"), equalTo(new BigInteger("18446744073709551615")));
        assertThat(convertedMap.get("counts"), equalTo(List.of(1L, 9_007_199_254_740_993L)));
        assertThat(convertedMap.get("countsByName"), equalTo(Map.of("first", 7L)));
    }

    @Test
    void convert_withTimestampInRange_returnsRfc3339String() throws Exception {
        final Timestamp timestamp = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();

        assertThat(objectUnderTest.convert(timestamp), equalTo("2023-11-14T22:13:20Z"));
    }

    @Test
    void convert_withStruct_returnsJsonMapping() throws Exception {
        final Struct struct = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("test").build())
                .putFields("missing", Value.newBuilder().setNullValueValue(0).build())
                .build();

        final Object convertedValue = objectUnderTest.convert(struct);

        assertThat(convertedValue, instanceOf(Map.class));
        final Map<String, Object> convertedMap = (Map<String, Object>) convertedValue;
        assertThat(convertedMap.get("name"), equalTo("test"));
        assertThat(convertedMap.containsKey("missing"), equalTo(true));
        assertThat(convertedMap.get("missing"), equalTo(null));
    }

    @Test
    void convert_withKnownAnyType_returnsExpandedValue() throws Exception {
        final Object convertedValue = objectUnderTest.convert(Any.pack(StringValue.of("test-value")));

        assertThat(convertedValue, instanceOf(Map.class));
        final Map<String, Object> convertedMap = (Map<String, Object>) convertedValue;
        assertThat(convertedMap.get("@type"), equalTo("type.googleapis.com/google.protobuf.StringValue"));
        assertThat(convertedMap.get("value"), equalTo("test-value"));
    }

    @Test
    void convert_withUnknownAnyType_returnsSafeGenericRepresentation() throws Exception {
        final byte[] payload = "unknown-payload".getBytes(StandardCharsets.UTF_8);
        final Any unknownAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/example.UnknownMessage")
                .setValue(ByteString.copyFrom(payload))
                .build();

        final Object convertedValue = objectUnderTest.convert(unknownAny);

        assertThat(convertedValue, instanceOf(Map.class));
        final Map<String, Object> convertedMap = (Map<String, Object>) convertedValue;
        assertThat(convertedMap.get("typeUrl"), equalTo("type.googleapis.com/example.UnknownMessage"));
        assertThat(convertedMap.get("value"), equalTo(Base64.getEncoder().encodeToString(payload)));
    }

    @Test
    void convert_withMixedAnyAndUnsignedValues_preservesValues() throws Exception {
        final Any knownAny = Any.pack(StringValue.of("known-value"));
        final Any unknownAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/example.UnknownMessage")
                .setValue(ByteString.copyFromUtf8("unknown-value"))
                .build();
        final DynamicMessage message = createFallbackMessage(knownAny, unknownAny);

        final Object convertedValue = objectUnderTest.convert(message);

        assertThat(convertedValue, instanceOf(Map.class));
        final Map<String, Object> convertedMap = (Map<String, Object>) convertedValue;
        assertThat(((Map<?, ?>) convertedMap.get("knownAny")).get("value"), equalTo("known-value"));
        assertThat(convertedMap.get("unsigned32"), equalTo(4_294_967_295L));
        assertThat(((Map<?, ?>) convertedMap.get("unknownAny")).get("typeUrl"), equalTo("type.googleapis.com/example.UnknownMessage"));
        assertThat(convertedMap.get("unsigned32"), equalTo(4_294_967_295L));
        assertThat(convertedMap.get("unsigned64"), equalTo(new BigInteger("18446744073709551615")));
        assertThat(convertedMap.get("enumValue"), equalTo(123));
    }

    @Test
    void convert_withOutOfRangeTimestamp_fallsBackToFields() throws Exception {
        final Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(1_700_000_000_000L)
                .setNanos(123)
                .build();

        final Object convertedValue = objectUnderTest.convert(timestamp);

        assertThat(convertedValue, instanceOf(Map.class));
        final Map<String, Object> convertedMap = (Map<String, Object>) convertedValue;
        assertThat(convertedMap.get("seconds"), equalTo(1_700_000_000_000L));
        assertThat(convertedMap.get("nanos"), equalTo(123));
    }

    private DynamicMessage createIntegerMessage() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.DescriptorProto entryDescriptor = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("CountsByNameEntry")
                .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("key")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("value")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .build();
        final DescriptorProtos.DescriptorProto messageDescriptor = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("IntegerMessage")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("count")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("signed_fixed")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("unsigned_max")
                        .setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("counts")
                        .setNumber(4)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("counts_by_name")
                        .setNumber(5)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".example.IntegerMessage.CountsByNameEntry")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                .addNestedType(entryDescriptor)
                .build();
        final DescriptorProtos.FileDescriptorProto fileDescriptor = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("integer_message.proto")
                .setPackage("example")
                .setSyntax("proto3")
                .addMessageType(messageDescriptor)
                .build();
        final Descriptors.Descriptor descriptor = Descriptors.FileDescriptor
                .buildFrom(fileDescriptor, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("IntegerMessage");
        final Descriptors.Descriptor entry = descriptor.findNestedTypeByName("CountsByNameEntry");
        final DynamicMessage mapEntry = DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "first")
                .setField(entry.findFieldByName("value"), 7L)
                .build();

        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("count"), 12L)
                .setField(descriptor.findFieldByName("signed_fixed"), -5L)
                .setField(descriptor.findFieldByName("unsigned_max"), -1L)
                .addRepeatedField(descriptor.findFieldByName("counts"), 1L)
                .addRepeatedField(descriptor.findFieldByName("counts"), 9_007_199_254_740_993L)
                .addRepeatedField(descriptor.findFieldByName("counts_by_name"), mapEntry)
                .build();
    }

    private DynamicMessage createFallbackMessage(final Any knownAny, final Any unknownAny)
            throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.DescriptorProto messageDescriptor = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("FallbackMessage")
                .addField(createAnyField("known_any", 1))
                .addField(createAnyField("unknown_any", 2))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("unsigned32")
                        .setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("unsigned64")
                        .setNumber(4)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("enum_value")
                        .setNumber(5)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                        .setTypeName(".example.FallbackMessage.TestEnum"))
                .addEnumType(DescriptorProtos.EnumDescriptorProto.newBuilder()
                        .setName("TestEnum")
                        .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                                .setName("TEST_ENUM_UNSPECIFIED")
                                .setNumber(0)))
                .build();
        final DescriptorProtos.FileDescriptorProto fileDescriptor = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("fallback_message.proto")
                .setPackage("example")
                .setSyntax("proto3")
                .addDependency(AnyProto.getDescriptor().getName())
                .addMessageType(messageDescriptor)
                .build();
        final Descriptors.Descriptor descriptor = Descriptors.FileDescriptor
                .buildFrom(fileDescriptor, new Descriptors.FileDescriptor[] {AnyProto.getDescriptor()})
                .findMessageTypeByName("FallbackMessage");

        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("known_any"), knownAny)
                .setField(descriptor.findFieldByName("unknown_any"), unknownAny)
                .setField(descriptor.findFieldByName("unsigned32"), -1)
                .setField(descriptor.findFieldByName("unsigned64"), -1L)
                .setField(
                        descriptor.findFieldByName("enum_value"),
                        descriptor.findFieldByName("enum_value").getEnumType().findValueByNumberCreatingIfUnknown(123))
                .build();
    }

    private DescriptorProtos.FieldDescriptorProto createAnyField(final String name, final int number) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Any")
                .build();
    }
}
