package lambda;

import com.evolvedbinary.j8fu.tuple.Tuple3;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock.LockMode;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BasicLambdaApi {

    public static <CR, CDR, DR> Tuple3<Optional<CR>, Optional<CDR>, Optional<DR>> execute(final BrokerPool brokerPool,
            final XmldbURI collectionUri, final LockMode collectionLockMode, final Optional<Function<Collection, CR>> collectionFun,
            final Function<Collection, XmldbURI> docUriMapper, final LockMode documentLockMode, final Optional<BiFunction<Collection, DocumentImpl, CDR>> collectionDocumentFun,
            final Optional<Function<DocumentImpl, DR>> documentFun) throws EXistException, PermissionDeniedException, LockException {

        try(final DBBroker broker = brokerPool.getBroker()) {
            Collection collection = null;
            DocumentImpl document = null;

            final Optional<CR> cr;
            final Optional<CDR> cdr;
            final Optional<DR> dr;

            try {
                collection = broker.openCollection(collectionUri, collectionLockMode);

                final Collection c = collection;
                cr = collectionFun.map(cf -> cf.apply(c));     // with just collection lock

                final XmldbURI docUri = docUriMapper.apply(collection);

                try {
                    document = collection.getDocumentWithLock(broker, docUri, documentLockMode);

                    final DocumentImpl d = document;
                    cdr = collectionDocumentFun.map(cdf -> cdf.apply(c, d));    // with collection and document lock

                    // NOTE: we can release the collection lock early
                    collection.release(collectionLockMode);
                    collection = null;

                    dr = documentFun.map(df -> df.apply(d));      // with just the document lock

                } finally {
                    if(document != null) {
                        document.getUpdateLock().release(documentLockMode);
                    }
                }

            } finally {
                if(collection != null) {
                    collection.release(collectionLockMode);
                }
            }

            return new Tuple3<>(cr, cdr, dr);
        }
    }
}
