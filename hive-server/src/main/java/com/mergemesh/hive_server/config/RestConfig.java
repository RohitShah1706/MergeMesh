package com.mergemesh.hive_server.config;

import org.springframework.web.client.RestTemplate;

public class RestConfig {
    private RestTemplate restTemplate;

    public RestTemplate getRestTemplate() {
        if (restTemplate == null) {
            restTemplate = new RestTemplate();
        }

        return restTemplate;
    }
}
