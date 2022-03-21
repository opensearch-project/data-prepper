package com.amazon.dataprepper.model.metric;

import java.util.List;

/**
 * A summary, representing a metric event.
 * @since 1.4
 */
public interface Summary extends Metric {

    /**
     * Gets the quantiles for a summary
     * @return the quantiles
     * @since 1.4
     */
    List<JacksonSummary.SummaryQuantile> getQuantiles();

    /**
     * Gets the number of quantiles for a summary
     * @return the number of quantiles
     * @since 1.4
     */
    Integer getQuantileValuesCount();

    /**
     * Gets the value for a summary
     * @return the sum
     * @since 1.4
     */
    Double getSum();
}
