package twr;

import org.exist.collections.Collection;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.xmldb.XmldbURI;

import java.util.function.BiFunction;

/**
 * Offers Asymmetrical Locking through try-with-resources
 * by wrapping objects and managing their lock acquire/release.
 */
public class ManagedFactory {
    private ManagedFactory() {
    }

    public static ManagedCollection manage(final BrokerPool brokerPool, final XmldbURI collectionName, final Lock.LockMode collectionLockMode) {
        return new ManagedCollection(brokerPool, collectionName, collectionLockMode);
    }

    public static ManagedCollectionAndDocument manage(final BrokerPool brokerPool, final XmldbURI collectionName, final Lock.LockMode collectionLockMode, final BiFunction<DBBroker, Collection, XmldbURI> documentUriSupplier, final Lock.LockMode documentLockMode) {
        return new ManagedCollectionAndDocument(brokerPool, collectionName, collectionLockMode, documentUriSupplier, documentLockMode);
    }
}
