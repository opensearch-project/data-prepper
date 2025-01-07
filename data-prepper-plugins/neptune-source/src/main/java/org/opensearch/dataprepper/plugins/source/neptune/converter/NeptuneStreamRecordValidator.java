/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.converter;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.stream.StreamUtils;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphData;
import software.amazon.awssdk.services.neptunedata.model.SparqlData;

import java.io.IOException;


/**
 * Validates if the record from Neptune Streams is a valid record.
 * (1) If enableNonStringIndexing in {@link NeptuneSourceConfig} is true, then all datatypes are valid and mapped to
 * OS datatypes as defined in <a href="https://docs.aws.amazon.com/neptune/latest/userguide/full-text-search-non-string-indexing-mapping.html">Mapping of SPARQL and Gremlin datatypes to OpenSearch</a>
 * (2) Otherwise, only String datatypes are supported and any non-string record is dropped.
 */
public class NeptuneStreamRecordValidator {
    private static final Logger LOG = LoggerFactory.getLogger(NeptuneStreamRecordValidator.class);
    private final boolean allowNonStringDatatypes;

    public NeptuneStreamRecordValidator(final boolean allowNonStringDatatypes) {
        this.allowNonStringDatatypes = allowNonStringDatatypes;
    }

    public boolean isValid(final NeptuneStreamRecord record) {
        if (record.getData() instanceof SparqlData) {
            return isValidSparqlRecord(record);
        }
        return isValidPropertyGraphRecord(record);
    }

    private boolean isValidPropertyGraphRecord(final NeptuneStreamRecord record) {
        if (allowNonStringDatatypes) {
            return true;
        }
        final PropertygraphData data = (PropertygraphData) record.getData();
        final String datatype = data.value().asMap().get("dataType").asString();
        return datatype.equalsIgnoreCase("String");
    }

    private boolean isValidSparqlRecord(final NeptuneStreamRecord record) {
        if (allowNonStringDatatypes) {
            return true;
        }
        final SparqlData data = (SparqlData) record.getData();
        try {
            final Statement statement = StreamUtils.parseSparqlStatement(data.stmt());
            if (!statement.getObject().isLiteral()) {
                return false;
            }
            return isSparqlStringDatatype(((Literal) statement.getObject()).getDatatype());
        } catch (IOException e) {
            LOG.error("Failed to parse Sparql statement, Skipping record: ", e);
            return false;
        }
    }

    private static boolean isSparqlStringDatatype(IRI datatype) {
        return XSD.STRING.equals(datatype) || RDF.LANGSTRING.equals(datatype);
    }
}
