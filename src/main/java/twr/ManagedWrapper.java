package twr;

import com.evolvedbinary.j8fu.function.RunnableE;

import java.util.function.Supplier;

public class ManagedWrapper<T> implements AutoCloseable {
    private final T resource;
    private final Supplier<Boolean> isClosedCheck;
    private final RunnableE<WrappedException> closer;

    ManagedWrapper(final T resource, final Supplier<Boolean> isClosedCheck, final RunnableE<WrappedException> closer) {
        this.resource = resource;
        this.isClosedCheck = isClosedCheck;
        this.closer = closer;
    }

    public T unwrap() {
        return resource;
    }

    public boolean isClosed() {
        return isClosedCheck.get();
    }

    @Override
    public void close() throws WrappedException {
        closer.run();
    }
}
