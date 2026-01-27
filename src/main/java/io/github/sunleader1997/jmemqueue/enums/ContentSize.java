package io.github.sunleader1997.jmemqueue.enums;

import static io.github.sunleader1997.jmemqueue.JSharedMemSegment.CONTENT_OFFSET;

public enum ContentSize {
    B_512(512 - CONTENT_OFFSET),
    KB_1(1024 - CONTENT_OFFSET),
    KB_2(2048 - CONTENT_OFFSET),
    KB_4(4096 - CONTENT_OFFSET),
    ;
    private final int size;

    ContentSize(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }
}