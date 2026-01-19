package io.github.sunleader1997.jmemqueue;

import java.io.File;

public class Dictionary {
    public static final String PARENT_DIR = System.getProperty("java.io.tmpdir") + File.separator + "JSMQ" + File.separator;

    static {
        new File(PARENT_DIR).mkdir();
    }
}
