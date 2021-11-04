package com.example.restservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.example.restservice.Logging;
import java.io.IOException;

import java.util.Optional;
import java.util.Random;

@RestController()
public class LoggingController {

    private static final Random RANDOM = new Random();

    @PostMapping("/logs")
    public String save(@RequestBody Logging logging) throws IOException {
        //TODO send logs to another system.
        // 1% of the time will throw exception
        /*if (RANDOM.nextInt(100) == 0) {
            throw new IOException("Dummy Exception");
        } else {
            return "Dummy";
        }*/
        return "Dummy";
    }
}