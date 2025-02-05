package com.example.worker.controller;

import com.example.worker.raft.JRaftServerHolder;
import com.example.worker.raft.KeyFrequencyStateMachine;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/key")
public class KeyController {
    private final JRaftServerHolder jRaftServerHolder;

    public KeyController(JRaftServerHolder jRaftServerHolder) {
        this.jRaftServerHolder = jRaftServerHolder;
    }

    @GetMapping("/hotkeys")
    public List<String> getHotKeys() {
        // 取前5作为热点Key演示
        KeyFrequencyStateMachine fsm = jRaftServerHolder.getServer().getFsm();
        ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> freqMap = fsm.getFreqMap();

        return freqMap.entrySet()
                .stream()
                .sorted((a,b) -> Long.compare(b.getValue().size(), a.getValue().size()))
                .limit(5)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    @GetMapping("/allkeys")
    public Map<String, Long> getAllKeys() {
        // 返回所有的Key计数
        KeyFrequencyStateMachine fsm = jRaftServerHolder.getServer().getFsm();
        // 转换为 Map<String, Long>，统计每个 key 的访问次数
        return fsm.getFreqMap().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey, // key 保持不变
                        entry -> (long) entry.getValue().size() // 计算访问次数
                ));
    }
}