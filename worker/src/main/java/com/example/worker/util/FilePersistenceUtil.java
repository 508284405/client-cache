package com.example.worker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

public class FilePersistenceUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FilePersistenceUtil.class);

    @SuppressWarnings("unchecked")
    public static ConcurrentHashMap<String, Long> loadFreqMapFromFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            LOG.info("Snapshot file not found: {}", path);
            return null;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            Object obj = ois.readObject();
            if (obj instanceof ConcurrentHashMap) {
                return (ConcurrentHashMap<String, Long>) obj;
            }
        } catch (Exception e) {
            LOG.error("Load freqMap file failed", e);
        }
        return null;
    }

    public static void saveFreqMapToFile(ConcurrentHashMap<String, Long> freqMap, String path) throws IOException {
        File file = new File(path);
        // 确保目录存在
        file.getParentFile().mkdirs();
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
            oos.writeObject(freqMap);
            oos.flush();
        }
        catch (IOException e) {
            throw e;
        }
    }
}