package io.github.sunleader1997.jmemqueue.ttl;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;

public class JCleaner {
    /**
     * 释放直接内存文件句柄
     * @param buffer
     */
    public static void clean(MappedByteBuffer buffer){
        try{
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);
            unsafe.invokeCleaner(buffer);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
