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

import java.util.Optional;

@RestController()
public class LoggingController {

    @PostMapping("/logs")
    public String save(@RequestBody Logging logging) throws IOException {
        //TODO send logs to another system.
        return "Dummy";
    }
}