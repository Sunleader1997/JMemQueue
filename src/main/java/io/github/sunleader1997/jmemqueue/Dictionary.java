package io.github.sunleader1997.jmemqueue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Dictionary {
    public static final Path PARENT_DIR = Paths.get(System.getProperty("java.io.tmpdir")).resolve("JSMQ");

    static {
        try {
            if (!Files.exists(PARENT_DIR)) {
                Files.createDirectory(PARENT_DIR);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path getTopicDir(String topic) {
        return PARENT_DIR.resolve(topic);
    }

    public static Path getAndMakeTopicDir(String topic) {
        Path dir = getTopicDir(topic);
        try {
            if (!Files.exists(dir)) {
                Files.createDirectory(dir);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dir;
    }

    public static void deleteTopic(String topic) {
        boolean delete = deleteDirectory(getTopicDir(topic).toFile());
        System.out.println("【DELETE】 TOPIC " + topic + " S " + delete);
    }

    public static boolean deleteDirectory(File directory) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!deleteDirectory(file)) {
                        return false;
                    }
                }
            }
        }
        return directory.delete();
    }
}
