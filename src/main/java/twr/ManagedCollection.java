package twr;

import com.evolvedbinary.j8fu.lazy.LazyValE;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.xmldb.XmldbURI;

import javax.annotation.Nullable;

import static com.evolvedbinary.j8fu.Either.Left;
import static com.evolvedbinary.j8fu.Either.Right;

public class ManagedCollection implements Managed<Collection> {
    @Nullable private LazyValE<Collection, WrappedException> lazyCollection;
    private final Lock.LockMode collectionLockMode;

    ManagedCollection(final BrokerPool brokerPool, final XmldbURI collectionUri, final Lock.LockMode collectionLockMode) {
        this.lazyCollection = new LazyValE<>(() -> {
            try (final DBBroker broker = brokerPool.getBroker()) {
                return Right(broker.openCollection(collectionUri, collectionLockMode));
            } catch (final EXistException | PermissionDeniedException e) {
                return Left(new WrappedException(e));
            }
        });
        this.collectionLockMode = collectionLockMode;
    }

    public ManagedWrapper<Collection> withCollection() throws WrappedException, IllegalStateException {
        throwIfClosed();
        return new ManagedWrapper<>(lazyCollection.get(), this::isClosed, this::close);  // NOTE: we close the Collection upon completion of this function
    }

    @Override
    public boolean isClosed() {
        return lazyCollection == null;
    }

    @Override
    public void close() throws WrappedException {
        if(isClosed()) {
            return;  // already closed
        }

        if(lazyCollection.isInitialized()) {
            lazyCollection.get().release(collectionLockMode);
        }
        lazyCollection = null;
    }

    private void throwIfClosed() throws IllegalStateException {
        if(lazyCollection == null) {
            throw new IllegalStateException("Collection is closed");
        }
    }
}
