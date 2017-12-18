package twr;

public interface Managed<T> extends AutoCloseable {
    boolean isClosed();
}
