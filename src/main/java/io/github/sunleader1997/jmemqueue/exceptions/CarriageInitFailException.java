package io.github.sunleader1997.jmemqueue.exceptions;

public class CarriageInitFailException extends RuntimeException {
    /** Constructs a new runtime exception with {@code null} as its
     * detail message.  The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     */
    public CarriageInitFailException() {
        super();
    }
    public CarriageInitFailException(Throwable cause) {
        super(cause);
    }
}
