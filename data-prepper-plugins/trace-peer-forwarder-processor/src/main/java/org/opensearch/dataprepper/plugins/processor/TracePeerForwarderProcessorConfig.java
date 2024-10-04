package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;

@JsonClassDescription("The <code>trace_peer_forwarder</code> processor is used with peer forwarder to reduce by half " +
        "the number of events forwarded in a <a href=\"https://opensearch.org/docs/latest/data-prepper/common-use-cases/trace-analytics/\">Trace Analytics</a> pipeline.")
public class TracePeerForwarderProcessorConfig {
}
