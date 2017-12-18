package twr;

import com.evolvedbinary.j8fu.tuple.Tuple2;
import org.easymock.IMocksControl;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.lock.Lock.LockMode;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.exist.storage.lock.Lock.LockMode.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ManagedTest {

    @Test
    public void managedCollection() throws WrappedException, EXistException, PermissionDeniedException {
        final XmldbURI col1Uri = XmldbURI.create("col1_name");
        final LockMode col1LockMode = READ_LOCK;

        final IMocksControl ctrl = createStrictControl();
        ctrl.checkOrder(true);

        final BrokerPool mockBrokerPool = ctrl.createMock(BrokerPool.class);
        final DBBroker mockBroker = ctrl.createMock(DBBroker.class);
        final Collection mockCollection = ctrl.createMock(Collection.class);

        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBroker.openCollection(col1Uri, col1LockMode)).andReturn(mockCollection);
        mockBroker.close();
        mockCollection.release(col1LockMode);


        ctrl.replay();

        try(final ManagedCollection mcol1 = ManagedFactory.manage(mockBrokerPool, col1Uri, col1LockMode)) {

            assertFalse(mcol1.isClosed());

            try(final ManagedWrapper<Collection> wcol1 = mcol1.withCollection()) {
                assertFalse(mcol1.isClosed());
                assertFalse(wcol1.isClosed());
                assertEquals(mockCollection, wcol1.unwrap());
            }

            assertTrue(mcol1.isClosed());
        }

        ctrl.verify();
    }

    @Test
    public void managedCollectionAndDocument_all() throws WrappedException, EXistException, PermissionDeniedException, LockException {
        final XmldbURI col1Uri = XmldbURI.create("col1_name");
        final LockMode col1LockMode = READ_LOCK;

        final XmldbURI doc1Uri = XmldbURI.create("doc1_name");
        final LockMode doc1LockMode = WRITE_LOCK;

        final IMocksControl ctrl = createStrictControl();
        ctrl.checkOrder(true);

        final BrokerPool mockBrokerPool = ctrl.createMock(BrokerPool.class);
        final DBBroker mockBroker = ctrl.createMock(DBBroker.class);
        final Collection mockCollection = ctrl.createMock(Collection.class);
        final DocumentImpl mockDocument = ctrl.createMock(DocumentImpl.class);
        final Lock mockDocLock = ctrl.createMock(Lock.class);

        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBroker.openCollection(col1Uri, col1LockMode)).andReturn(mockCollection);
        mockBroker.close();
        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockCollection.getDocumentWithLock(mockBroker, doc1Uri, doc1LockMode)).andReturn(mockDocument);
        mockBroker.close();
        mockCollection.release(col1LockMode);
        expect(mockDocument.getUpdateLock()).andReturn(mockDocLock);
        mockDocLock.release(doc1LockMode);

        ctrl.replay();

        try(final ManagedCollectionAndDocument mcoldoc1 = ManagedFactory.manage(mockBrokerPool, XmldbURI.create("col1_name"), READ_LOCK, (broker, col) -> XmldbURI.create("doc1_name"), Lock.LockMode.WRITE_LOCK)) {

            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<Collection> wcol1 = mcoldoc1.withCollection()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wcol1.isClosed());
                assertEquals(mockCollection, wcol1.unwrap());
            }

            // should not yet be closed
            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<Tuple2<Collection, DocumentImpl>> wcoldoc1 = mcoldoc1.withCollectionAndDocument()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wcoldoc1.isClosed());
                assertEquals(mockCollection, wcoldoc1.unwrap()._1);
                assertEquals(mockDocument, wcoldoc1.unwrap()._2);
            }

            // should not yet be closed
            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<DocumentImpl> wdoc1 = mcoldoc1.withDocument()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wdoc1.isClosed());
                assertEquals(mockDocument, wdoc1.unwrap());
            }

            // should NOW be closed
            assertTrue(mcoldoc1.isClosed());
        }

        ctrl.verify();
    }

    @Test
    public void managedCollectionAndDocument_withCollectionAndDocumentOnly() throws WrappedException, EXistException, PermissionDeniedException, LockException {
        final XmldbURI col1Uri = XmldbURI.create("col1_name");
        final LockMode col1LockMode = READ_LOCK;

        final XmldbURI doc1Uri = XmldbURI.create("doc1_name");
        final LockMode doc1LockMode = WRITE_LOCK;

        final IMocksControl ctrl = createStrictControl();
        ctrl.checkOrder(true);

        final BrokerPool mockBrokerPool = ctrl.createMock(BrokerPool.class);
        final DBBroker mockBroker = ctrl.createMock(DBBroker.class);
        final Collection mockCollection = ctrl.createMock(Collection.class);
        final DocumentImpl mockDocument = ctrl.createMock(DocumentImpl.class);
        final Lock mockDocLock = ctrl.createMock(Lock.class);

        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBroker.openCollection(col1Uri, col1LockMode)).andReturn(mockCollection);
        mockBroker.close();
        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockCollection.getDocumentWithLock(mockBroker, doc1Uri, doc1LockMode)).andReturn(mockDocument);
        mockBroker.close();
        mockCollection.release(col1LockMode);
        expect(mockDocument.getUpdateLock()).andReturn(mockDocLock);
        mockDocLock.release(doc1LockMode);

        ctrl.replay();

        try(final ManagedCollectionAndDocument mcoldoc1 = ManagedFactory.manage(mockBrokerPool, XmldbURI.create("col1_name"), READ_LOCK, (broker, col) -> XmldbURI.create("doc1_name"), Lock.LockMode.WRITE_LOCK)) {

            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<Tuple2<Collection, DocumentImpl>> wcoldoc1 = mcoldoc1.withCollectionAndDocument()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wcoldoc1.isClosed());
                assertEquals(mockCollection, wcoldoc1.unwrap()._1);
                assertEquals(mockDocument, wcoldoc1.unwrap()._2);
            }

            // should not yet be closed
            assertFalse(mcoldoc1.isClosed());
        }

        ctrl.verify();
    }

    @Test
    public void managedCollectionAndDocument_withDocumentOnly() throws WrappedException, EXistException, PermissionDeniedException, LockException {
        final XmldbURI col1Uri = XmldbURI.create("col1_name");
        final LockMode col1LockMode = READ_LOCK;

        final XmldbURI doc1Uri = XmldbURI.create("doc1_name");
        final LockMode doc1LockMode = WRITE_LOCK;

        final IMocksControl ctrl = createStrictControl();
        ctrl.checkOrder(true);

        final BrokerPool mockBrokerPool = ctrl.createMock(BrokerPool.class);
        final DBBroker mockBroker = ctrl.createMock(DBBroker.class);
        final Collection mockCollection = ctrl.createMock(Collection.class);
        final DocumentImpl mockDocument = ctrl.createMock(DocumentImpl.class);
        final Lock mockDocLock = ctrl.createMock(Lock.class);

        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBroker.openCollection(col1Uri, col1LockMode)).andReturn(mockCollection);
        mockBroker.close();
        expect(mockCollection.getDocumentWithLock(mockBroker, doc1Uri, doc1LockMode)).andReturn(mockDocument);
        mockBroker.close();
        mockCollection.release(col1LockMode);
        expect(mockDocument.getUpdateLock()).andReturn(mockDocLock);
        mockDocLock.release(doc1LockMode);

        ctrl.replay();

        try(final ManagedCollectionAndDocument mcoldoc1 = ManagedFactory.manage(mockBrokerPool, XmldbURI.create("col1_name"), READ_LOCK, (broker, col) -> XmldbURI.create("doc1_name"), Lock.LockMode.WRITE_LOCK)) {

            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<DocumentImpl> wdoc1 = mcoldoc1.withDocument()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wdoc1.isClosed());
                assertEquals(mockDocument, wdoc1.unwrap());
            }

            // should NOW be closed
            assertTrue(mcoldoc1.isClosed());
        }

        ctrl.verify();
    }

    @Test
    public void managedCollectionAndDocument_withCollectionAndDocument_withDocument() throws WrappedException, EXistException, PermissionDeniedException, LockException {
        final XmldbURI col1Uri = XmldbURI.create("col1_name");
        final LockMode col1LockMode = READ_LOCK;

        final XmldbURI doc1Uri = XmldbURI.create("doc1_name");
        final LockMode doc1LockMode = WRITE_LOCK;

        final IMocksControl ctrl = createStrictControl();
        ctrl.checkOrder(true);

        final BrokerPool mockBrokerPool = ctrl.createMock(BrokerPool.class);
        final DBBroker mockBroker = ctrl.createMock(DBBroker.class);
        final Collection mockCollection = ctrl.createMock(Collection.class);
        final DocumentImpl mockDocument = ctrl.createMock(DocumentImpl.class);
        final Lock mockDocLock = ctrl.createMock(Lock.class);

        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockBroker.openCollection(col1Uri, col1LockMode)).andReturn(mockCollection);
        mockBroker.close();
        expect(mockBrokerPool.getBroker()).andReturn(mockBroker);
        expect(mockCollection.getDocumentWithLock(mockBroker, doc1Uri, doc1LockMode)).andReturn(mockDocument);
        mockBroker.close();
        mockCollection.release(col1LockMode);
        expect(mockDocument.getUpdateLock()).andReturn(mockDocLock);
        mockDocLock.release(doc1LockMode);

        ctrl.replay();

        try(final ManagedCollectionAndDocument mcoldoc1 = ManagedFactory.manage(mockBrokerPool, XmldbURI.create("col1_name"), READ_LOCK, (broker, col) -> XmldbURI.create("doc1_name"), Lock.LockMode.WRITE_LOCK)) {

            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<Tuple2<Collection, DocumentImpl>> wcoldoc1 = mcoldoc1.withCollectionAndDocument()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wcoldoc1.isClosed());
                assertEquals(mockCollection, wcoldoc1.unwrap()._1);
                assertEquals(mockDocument, wcoldoc1.unwrap()._2);
            }

            // should not yet be closed
            assertFalse(mcoldoc1.isClosed());

            try(final ManagedWrapper<DocumentImpl> wdoc1 = mcoldoc1.withDocument()) {
                assertFalse(mcoldoc1.isClosed());
                assertFalse(wdoc1.isClosed());
                assertEquals(mockDocument, wdoc1.unwrap());
            }

            // should NOW be closed
            assertTrue(mcoldoc1.isClosed());
        }

        ctrl.verify();
    }

}
