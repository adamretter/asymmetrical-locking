package twr;

import com.evolvedbinary.j8fu.lazy.LazyValE;
import com.evolvedbinary.j8fu.tuple.Tuple2;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;

import javax.annotation.Nullable;
import java.util.function.BiFunction;

import static com.evolvedbinary.j8fu.Either.Left;
import static com.evolvedbinary.j8fu.Either.Right;

public class ManagedCollectionAndDocument implements Managed<Collection> {
    @Nullable private LazyValE<Collection, WrappedException> lazyCollection;
    private final Lock.LockMode collectionLockMode;
    @Nullable private LazyValE<DocumentImpl, WrappedException> lazyDocument;
    private final Lock.LockMode documentLockMode;

    ManagedCollectionAndDocument(final BrokerPool brokerPool, final XmldbURI collectionUri, final Lock.LockMode collectionLockMode, final BiFunction<DBBroker, Collection, XmldbURI> documentUriSupplier, final Lock.LockMode documentLockMode) {
        this.lazyCollection = new LazyValE<>(() -> {
            try (final DBBroker broker = brokerPool.getBroker()) {
                return Right(broker.openCollection(collectionUri, collectionLockMode));
            } catch (final EXistException | PermissionDeniedException e) {
                return Left(new WrappedException(e));
            }
        });
        this.collectionLockMode = collectionLockMode;

        this.lazyDocument = new LazyValE<>(() -> {
            throwIfCollectionClosed();
            try (final DBBroker broker = brokerPool.getBroker()) {
                final Collection collection = lazyCollection.get();
                final XmldbURI docUri = documentUriSupplier.apply(broker, collection);
                return Right(collection.getDocumentWithLock(broker, docUri, documentLockMode));
            } catch (final EXistException | PermissionDeniedException | LockException e) {
                return Left(new WrappedException(e));
            } catch (final WrappedException e) {
                return Left(e);
            }
        });
        this.documentLockMode = documentLockMode;
    }

    public ManagedWrapper<Collection> withCollection() throws WrappedException, IllegalStateException {
        throwIfCollectionClosed();
        return new ManagedWrapper<>(lazyCollection.get(), this::isCollectionClosed, () -> {});    // NOTE we don't close the Collection upon completion of the call to withCollection
    }

    public ManagedWrapper<Tuple2<Collection, DocumentImpl>> withCollectionAndDocument() throws WrappedException, IllegalStateException {
        throwIfCollectionClosed();
        throwIfDocumentClosed();
        return new ManagedWrapper<>(new Tuple2<>(lazyCollection.get(), lazyDocument.get()), this::isClosed, this::closeCollection);    // NOTE we do close the Collection upon completion of this function
    }

    public ManagedWrapper<DocumentImpl> withDocument() throws WrappedException, IllegalStateException {
        throwIfDocumentClosed();
        final DocumentImpl doc = lazyDocument.get();
        closeCollection();  // ensures that the Collection is closed, as order may have been either (1) withCollection() -> withDocument() (without the call to withCollectionAndDocument(), or (2) withDocument (without the calls to either withCollection() and/or withCollectionAndDocument())
        return new ManagedWrapper<>(doc, this::isDocumentClosed, this::closeDocument);  //NOTE we close the document upon completion of this function
    }

    @Override
    public boolean isClosed() {
        return isCollectionClosed() && isDocumentClosed();
    }

    private boolean isCollectionClosed() {
        return lazyCollection == null;
    }

    private boolean isDocumentClosed() {
        return lazyDocument == null;
    }

    @Override
    public void close() throws WrappedException {
        closeCollection();
        closeDocument();
    }

    private void closeCollection() throws WrappedException {
        if(isCollectionClosed()) {
            return;  // already closed
        }

        if(lazyCollection.isInitialized()) {
            lazyCollection.get().release(collectionLockMode);
        }
        lazyCollection = null;
    }

    private void closeDocument() throws WrappedException {
        if(isDocumentClosed()) {
            return;  // already closed
        }

        if(lazyDocument.isInitialized()) {
            lazyDocument.get().getUpdateLock().release(documentLockMode);
        }
        lazyDocument = null;
    }

    private void throwIfCollectionClosed() throws IllegalStateException {
        if(lazyCollection == null) {
            throw new IllegalStateException("Collection is closed");
        }
    }

    private void throwIfDocumentClosed() throws IllegalStateException {
        if(lazyDocument == null) {
            throw new IllegalStateException("Document is closed");
        }
    }
}
