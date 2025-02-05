package com.example.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WorkerNodeMain {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNodeMain.class);

    public static void main(String[] args) {
        SpringApplication.run(WorkerNodeMain.class, args);
    }
}