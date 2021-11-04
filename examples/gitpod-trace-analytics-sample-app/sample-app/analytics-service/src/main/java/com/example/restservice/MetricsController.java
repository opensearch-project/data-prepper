package com.example.restservice;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class MetricsController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @GetMapping("/metrics")
    public Metrics metrics(@RequestParam(value = "name", defaultValue = "Service-X") String name) {
        return new Metrics(counter.incrementAndGet(), String.format(template, name));
    }
}