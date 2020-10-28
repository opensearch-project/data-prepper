package com.example.restservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.example.restservice.Logging;
import java.io.IOException;

@RestController()
public class LoggingController {

    @Autowired
    private RestHighLevelClient client;

    @PostMapping("/logs")
    public String save(@RequestBody Logging logging) throws IOException {
        IndexRequest request = new IndexRequest("logs");
        request.id(logging.getService());
        request.source(new ObjectMapper().writeValueAsString(logging), XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }
}