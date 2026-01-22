package io.github.sunleader1997.jmemqueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.Buffer;
import java.nio.ByteOrder;

public class AtomicVarHandle {
    /**
     * VarHandle用于对ByteBuffer进行CAS操作
     */
    public static final VarHandle INT_HANDLE = MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
    private static final VarHandle LONG_HANDLE = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    public static int getInt(Buffer buffer, int offset) {
        return (int) INT_HANDLE.getVolatile(buffer, offset);
    }

    public static void setInt(Buffer buffer, int offset, int newState) {
        INT_HANDLE.setVolatile(buffer, offset, newState);
    }

    public static boolean compareAndSetInt(Buffer buffer, int offset, int expectedState, int newState) {
        return INT_HANDLE.compareAndSet(buffer, offset, expectedState, newState);
    }

    public static long getLong(Buffer buffer, int offset) {
        return (long) LONG_HANDLE.getVolatile(buffer, offset);
    }

    public static void setLong(Buffer buffer, int offset, long newState) {
        LONG_HANDLE.setVolatile(buffer, offset, newState);
    }

    public static boolean compareAndSetLong(Buffer buffer, int offset, long expectedState, long newState) {
        return LONG_HANDLE.compareAndSet(buffer, offset, expectedState, newState);
    }
}
