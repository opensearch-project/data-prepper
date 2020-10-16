package com.example.restservice;

public class Metrics {

    private final long id;
    private final String service;

    public Metrics(long id, String service) {
        this.id = id;
        this.service = service;
    }

    public long getId() {
        return id;
    }

    public String getService() {
        return service;
    }
}