/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A security-hardened wrapper around {@link GlueSchemaRegistryKafkaDeserializer} that validates
 * the {@code className} field in JSON schemas before allowing deserialization.
 * <p>
 * This mitigates an unvalidated class-loading vulnerability where an attacker can register a
 * malicious schema with an arbitrary {@code className} in AWS Glue Schema Registry, causing
 * the underlying library to invoke {@code Class.forName(className)} with attacker-controlled input.
 * <p>
 * Without this validation, the vulnerability enables:
 * <ul>
 *   <li>Silent permanent record loss (confirmed)</li>
 *   <li>Potential Remote Code Execution via gadget classes (theoretical, gated by JDK 17 module system)</li>
 * </ul>
 *
 * @see <a href="https://github.com/awslabs/aws-glue-schema-registry/pull/533">Upstream PR #533</a>
 */
public class SafeGlueSchemaRegistryKafkaDeserializer implements Deserializer<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(SafeGlueSchemaRegistryKafkaDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final List<String> DEFAULT_ALLOWED_CLASS_PREFIXES = List.of(
            "java.lang.",
            "java.util.",
            "java.math.",
            "org.apache.avro.",
            "com.fasterxml.jackson."
    );

    private final GlueSchemaRegistryKafkaDeserializer delegate;
    private final List<String> allowedClassPrefixes;

    /**
     * Creates a safe deserializer wrapping the provided delegate with default allowed class prefixes.
     *
     * @param credentialsProvider AWS credentials provider
     * @param configs             configuration map for the Glue Schema Registry
     */
    public SafeGlueSchemaRegistryKafkaDeserializer(final AwsCredentialsProvider credentialsProvider,
                                                   final Map<String, ?> configs) {
        this(credentialsProvider, configs, DEFAULT_ALLOWED_CLASS_PREFIXES);
    }

    /**
     * Creates a safe deserializer with custom allowed class prefixes.
     *
     * @param credentialsProvider  AWS credentials provider
     * @param configs              configuration map for the Glue Schema Registry
     * @param allowedClassPrefixes list of allowed class name prefixes for JSON schema className validation
     */
    public SafeGlueSchemaRegistryKafkaDeserializer(final AwsCredentialsProvider credentialsProvider,
                                                   final Map<String, ?> configs,
                                                   final List<String> allowedClassPrefixes) {
        this.delegate = new GlueSchemaRegistryKafkaDeserializer(credentialsProvider, configs);
        this.allowedClassPrefixes = Collections.unmodifiableList(allowedClassPrefixes);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        delegate.configure(configs, isKey);
    }

    /**
     * Deserializes the given data after validating that the schema does not contain
     * a dangerous {@code className} field. If a className is present and does not match
     * the allowlist, deserialization is rejected with an exception rather than silently dropping.
     *
     * @param topic Kafka topic name
     * @param data  serialized data bytes
     * @return deserialized object
     * @throws AWSSchemaRegistryException if className validation fails
     */
    @Override
    public Object deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        validateSchemaClassName(data, topic);
        return delegate.deserialize(topic, data);
    }

    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Returns the underlying delegate deserializer for cases where direct access is needed
     * (e.g., accessing the deserialization facade for configuration inspection).
     *
     * @return the wrapped GlueSchemaRegistryKafkaDeserializer
     */
    public GlueSchemaRegistryKafkaDeserializer getDelegate() {
        return delegate;
    }

    private void validateSchemaClassName(final byte[] data, final String topic) {
        try {
            final Schema schema = delegate.getGlueSchemaRegistryDeserializationFacade().getSchema(data);
            if (schema == null) {
                return;
            }

            final String schemaDefinition = schema.getSchemaDefinition();
            if (schemaDefinition == null || schemaDefinition.isEmpty()) {
                return;
            }

            final String dataFormat = schema.getDataFormat();
            if (!"JSON".equalsIgnoreCase(dataFormat)) {
                return;
            }

            final JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaDefinition);
            final JsonNode classNameNode = schemaNode.get("className");
            if (classNameNode == null || classNameNode.isNull()) {
                return;
            }

            final String className = classNameNode.asText();
            if (!isClassNameAllowed(className)) {
                final String message = String.format(
                        "Blocked potentially dangerous className '%s' in Glue Schema Registry JSON schema for topic '%s'. " +
                                "The className does not match any allowed prefix. This may indicate a malicious schema registration.",
                        className, topic);
                LOG.error(message);
                throw new AWSSchemaRegistryException(message);
            }

            LOG.debug("Validated className '{}' for topic '{}'", className, topic);
        } catch (AWSSchemaRegistryException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Unable to validate schema className for topic '{}'. Proceeding with deserialization. Error: {}",
                    topic, e.getMessage());
        }
    }

    private boolean isClassNameAllowed(final String className) {
        if (className == null || className.isEmpty()) {
            return false;
        }

        for (final String prefix : allowedClassPrefixes) {
            if (className.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
