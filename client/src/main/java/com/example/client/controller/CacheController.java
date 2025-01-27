package com.example.client.controller;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cache")
public class CacheController {
    @GetMapping("/get")
    @Cacheable(value = "cache", key = "#key")
    public String get(String key) {
        return "value" + key;
    }
}
