package lambda;

import com.evolvedbinary.j8fu.tuple.Tuple2;
import com.evolvedbinary.j8fu.tuple.Tuple3;
import org.easymock.IMocksControl;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.DocumentMetadata;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.lock.Lock.LockMode;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.junit.Test;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.easymock.EasyMock.*;
import static org.exist.storage.lock.Lock.LockMode.*;
import static org.junit.Assert.assertEquals;

public class BasicLambdaApiTest {

    @Test
    public void testLambda() throws PermissionDeniedException, LockException, EXistException {
        final XmldbURI colName = XmldbURI.create("col1_name");
        final LockMode colLockMode = READ_LOCK;
        final long creationTime = 1234;
        final XmldbURI docName = XmldbURI.create("doc1_name");
        final LockMode docLockMode = WRITE_LOCK;
        final long lastModified = 5678;

        final IMocksControl ctrl = createStrictControl();
        ctrl.checkOrder(true);

        final BrokerPool mockBrokerPool = ctrl.createMock(BrokerPool.class);
        final DBBroker mockBroker = ctrl.createMock(DBBroker.class);
        final Collection mockCollection = ctrl.createMock(Collection.class);
        final DocumentImpl mockDocument = ctrl.createMock(DocumentImpl.class);
        final DocumentMetadata mockMetadata = ctrl.createMock(DocumentMetadata.class);
        final Lock mockDocLock = ctrl.createMock(Lock.class);

        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBroker.openCollection(colName, colLockMode)).andReturn(mockCollection);
        expect(mockCollection.getCreationTime()).andReturn(creationTime);
        expect(mockCollection.getDocumentWithLock(mockBroker, docName, docLockMode)).andReturn(mockDocument);
        expect(mockCollection.getURI()).andReturn(colName);
        expect(mockDocument.getFileURI()).andReturn(docName);
        mockCollection.release(colLockMode);    //NOTE: collection lock is released before doc lock
        expect(mockDocument.getMetadata()).andReturn(mockMetadata);
        mockMetadata.setLastModified(lastModified);
        expect(mockDocument.getUpdateLock()).andReturn(mockDocLock);
        mockDocLock.release(docLockMode);
        mockBroker.close();


        final Function<Collection, Long> colFun = collection -> {
            return collection.getCreationTime();
        };

        final BiFunction<Collection, DocumentImpl, String> colDocFun = (collection, document) -> {
            return collection.getURI().append(document.getFileURI()).toString();
        };

        final Function<DocumentImpl, Long> docFun = document -> {
            document.getMetadata().setLastModified(lastModified);
            return lastModified;
        };



        ctrl.replay();

        final Tuple3<Optional<Long>, Optional<String>, Optional<Long>> results = BasicLambdaApi.execute(mockBrokerPool,
                colName, colLockMode,
                Optional.of(colFun),
                collection -> docName, docLockMode,
                Optional.of(colDocFun),
                Optional.of(docFun)
        );

        assertEquals(creationTime, results._1.get().longValue());
        assertEquals(colName + "/" + docName, results._2.get());
        assertEquals(lastModified, results._3.get().longValue());

        ctrl.verify();
    }
}
