package com.example.client.lettuce;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringObjectRedisCodec implements RedisCodec<String, Object> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    public final static StringObjectRedisCodec INSTANCE = new StringObjectRedisCodec();

    StringObjectRedisCodec() {
    }

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return StandardCharsets.UTF_8.decode(bytes).toString();
    }

    @Override
    public Object decodeValue(ByteBuffer bytes) {
        String json = StandardCharsets.UTF_8.decode(bytes).toString();
        try {
            return objectMapper.readValue(json,Object.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error decoding JSON", e);
        }
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return StandardCharsets.UTF_8.encode(key);
    }

    @Override
    public ByteBuffer encodeValue(Object value) {
        try {
            String json = objectMapper.writeValueAsString(value);
            return StandardCharsets.UTF_8.encode(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error encoding JSON", e);
        }
    }
}