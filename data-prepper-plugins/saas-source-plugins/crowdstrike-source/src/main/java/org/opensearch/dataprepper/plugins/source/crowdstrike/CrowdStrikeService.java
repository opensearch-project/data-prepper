package org.opensearch.dataprepper.plugins.source.crowdstrike;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeApiResponse;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeIndicatorResult;
import org.opensearch.dataprepper.plugins.source.crowdstrike.rest.CrowdStrikeRestClient;
import org.opensearch.dataprepper.plugins.source.crowdstrike.utils.CrowdStrikeNextLinkValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;
import javax.inject.Named;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.BATCH_SIZE;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.FILTER_KEY;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.LAST_UPDATED;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.LIMIT_KEY;

/**
 * This service manages the interaction with CrowdStrike's API endpoints, handles pagination
 * and provides methods for fetching indicators from CrowdStrike.
 */
@Named
public class CrowdStrikeService {

    private final CrowdStrikeRestClient crowdStrikeRestClient;
    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeService.class);
    private static final String BASE_URL = "https://api.crowdstrike.com/";
    private static final String COMBINED_URL = "https://api.crowdstrike.com/intel/combined/indicators/v1";
    private final Timer searchCallLatencyTimer;


    public CrowdStrikeService(CrowdStrikeRestClient crowdStrikeRestClient, PluginMetrics pluginMetrics) {
        this.crowdStrikeRestClient = crowdStrikeRestClient;
        this.searchCallLatencyTimer = pluginMetrics.timer("searchCallLatencyTimer");
    }

    /**
     * Retrieves all indicator data from CrowdStrike in a paginated fashion.
     *   @param startTime       The start timestamp (inclusive) for filtering indicators.
     *   @param endTime         The end timestamp (exclusive) for filtering indicators.
     *   @param paginationLink  An optional pagination URL suffix (used when fetching next pages).
     *   @return                A {@link CrowdStrikeApiResponse} containing response body and headers.
     */
    public CrowdStrikeApiResponse getAllContent(Long startTime, Long endTime, String paginationLink) {
        URI uri = buildCrowdStrikeUri(startTime, endTime, paginationLink);

        return searchCallLatencyTimer.record(() -> {
            try {
                log.debug("Calling CrowdStrike API with URI: {}", uri);
                ResponseEntity<CrowdStrikeIndicatorResult> responseEntity = crowdStrikeRestClient.invokeGetApi(uri, CrowdStrikeIndicatorResult.class);

                CrowdStrikeApiResponse response = new CrowdStrikeApiResponse();
                response.setBody(responseEntity.getBody());
                response.setHeaders(responseEntity.getHeaders());
                return response;
            } catch (Exception e) {
                log.error("Error fetching CrowdStrike content from URI: {}", uri, e);
                throw new RuntimeException("CrowdStrike API call failed", e);
            }
        });
    }

    protected URI buildCrowdStrikeUri(Long startTime, Long endTime, String paginationLink) {
        try {
            if (paginationLink != null) {
                String urlString = BASE_URL + paginationLink;
                urlString = CrowdStrikeNextLinkValidator.validateAndSanitizeURL(urlString);
                return new URI(urlString);
            } else {
                // Manually construct and encode the query string
                String filter1 = URLEncoder.encode(LAST_UPDATED + ":>=" + startTime, StandardCharsets.UTF_8);
                String filter2 = URLEncoder.encode(LAST_UPDATED + ":<" + endTime, StandardCharsets.UTF_8);
                String encodedFilter = filter1 + "%2B" + filter2;  // Use literal '+' // ensure literal '+'

                UriComponentsBuilder builder = UriComponentsBuilder
                        .fromHttpUrl(COMBINED_URL)
                        .queryParam(FILTER_KEY, encodedFilter)
                        .queryParam(LIMIT_KEY, BATCH_SIZE);

                return builder.build(true).toUri();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct CrowdStrike request URI", e);
        }
    }
}
