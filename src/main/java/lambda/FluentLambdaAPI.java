/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2017 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

package lambda;

import com.evolvedbinary.j8fu.tuple.Tuple2;
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

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class FluentLambdaAPI {

    public static XmldbURI uri(final String uri) {
        return XmldbURI.create(uri);
    }

    public static FluentBrokerAPIBuilder builder(final BrokerPool brokerPool) {
        return new FluentBrokerAPIBuilder(brokerPool);
    }

    public static class FluentBrokerAPIBuilder {
        private final BrokerPool brokerPool;

        private FluentBrokerAPIBuilder(final BrokerPool brokerPool) {
            this.brokerPool = brokerPool;
        }

        public FluentBrokerAPIBuilder_Col1 withCollection(final XmldbURI collectionUri, final LockMode collectionLockMode) {
            return new FluentBrokerAPIBuilder_Col1(collectionUri, collectionLockMode);
        }

//        public FluentBrokerAPIBuilder_Col1 withCollections(final Tuple2<XmldbURI, LockMode>... collectionsAndLockModes) {
//            return new FluentBrokerAPIBuilder_ColN(collectionsAndLockModes);
//        }

        public class FluentBrokerAPIBuilder_Col1 {
            private final XmldbURI collectionUri;
            private final LockMode collectionLockMode;

            private FluentBrokerAPIBuilder_Col1(final XmldbURI collectionUri, final LockMode collectionLockMode) {
                this.collectionUri = collectionUri;
                this.collectionLockMode = collectionLockMode;
            }

            public <CR> FluentBrokerAPIBuilder_Col1_Exec<CR> execute(final Function<Collection, CR> collectionOp) {
                return new FluentBrokerAPIBuilder_Col1_Exec<>(collectionOp);
            }

            public FluentBrokerAPIBuilder_Col1_NoExec_Doc1 withDocument(final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun) {
                return new FluentBrokerAPIBuilder_Col1_NoExec_Doc1(documentLookupFun);
            }

            public class FluentBrokerAPIBuilder_Col1_Exec<CR> {
                private Function<Collection, CR> collectionOp;

                private FluentBrokerAPIBuilder_Col1_Exec(final Function<Collection, CR> collectionOp) {
                    this.collectionOp = collectionOp;
                }

                public FluentBrokerAPIBuilder_Col1_Exec_Doc1 withDocument(final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun) {
                    return new FluentBrokerAPIBuilder_Col1_Exec_Doc1(documentLookupFun);
                }

                public CR doAll() throws PermissionDeniedException, LockException, EXistException {
                    final Tuple3<Optional<CR>, Optional<Void>, Optional<Void>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.of(collectionOp), null, Optional.empty(), Optional.empty());
                    return result._1.get();
                }

                public class FluentBrokerAPIBuilder_Col1_Exec_Doc1 {
                    private final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun;

                    private FluentBrokerAPIBuilder_Col1_Exec_Doc1(final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun) {
                        this.documentLookupFun = documentLookupFun;
                    }

                    public <CDR> FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec<CDR> execute(final BiFunction<Collection, DocumentImpl, CDR> collectionDocumentOp) {
                        return new FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec<>(collectionDocumentOp);
                    }

                    public FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec withoutCollection() {
                        return new FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec();
                    }

                    public class FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec<CDR> {
                        private final BiFunction<Collection, DocumentImpl, CDR> collectionDocumentOp;

                        private FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec(final BiFunction<Collection, DocumentImpl, CDR> collectionDocumentOp) {
                            this.collectionDocumentOp = collectionDocumentOp;
                        }

                        public FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1 withoutCollection() {
                            return new FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1();
                        }

                        public Tuple2<CR, CDR> doAll() throws PermissionDeniedException, LockException, EXistException {
                            final Tuple3<Optional<CR>, Optional<CDR>, Optional<Void>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.of(collectionOp), documentLookupFun, Optional.of(collectionDocumentOp), Optional.empty());
                            return new Tuple2<>(result._1.get(), result._2.get());
                        }

                        public class FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1 {
                            private FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1() {}

                            public <DR> FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1_Exec<DR> execute(final Function<DocumentImpl, DR> documentOp) {
                                return new FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1_Exec<>(documentOp);
                            }
                        }

                        public class FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1_Exec<DR> {
                            private final Function<DocumentImpl, DR> documentOp;

                            private FluentBrokerAPIBuilder_Col1_Exec_Doc1_Exec_Doc1_Exec(final Function<DocumentImpl, DR> documentOp) {
                                this.documentOp = documentOp;
                            }

                            public Tuple3<CR, CDR, DR> doAll() throws PermissionDeniedException, LockException, EXistException {
                                final Tuple3<Optional<CR>, Optional<CDR>, Optional<DR>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.of(collectionOp), documentLookupFun, Optional.of(collectionDocumentOp), Optional.of(documentOp));
                                return new Tuple3<>(result._1.get(), result._2.get(), result._3.get());
                            }
                        }
                    }

                    public class FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec {
                        private FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec() {}

                        public <DR> FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec_Exec<DR> execute(final Function<DocumentImpl, DR> documentOp) {
                            return new FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec_Exec<>(documentOp);
                        }

                        public CR doAll() throws PermissionDeniedException, LockException, EXistException {
                            final Tuple3<Optional<CR>, Optional<Void>, Optional<Void>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.of(collectionOp), documentLookupFun, Optional.empty(), Optional.empty());
                            return result._1.get();
                        }

                        public class FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec_Exec<DR> {
                            private final Function<DocumentImpl, DR> documentOp;

                            private FluentBrokerAPIBuilder_Col1_Exec_Doc1_NoExec_Exec(final Function<DocumentImpl, DR> documentOp) {
                                this.documentOp = documentOp;
                            }

                            public Tuple2<CR, DR> doAll() throws PermissionDeniedException, LockException, EXistException {
                                final Tuple3<Optional<CR>, Optional<Void>, Optional<DR>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.of(collectionOp), documentLookupFun, Optional.empty(), Optional.of(documentOp));
                                return new Tuple2<>(result._1.get(), result._3.get());
                            }
                        }
                    }
                }
            }

            public class FluentBrokerAPIBuilder_Col1_NoExec_Doc1 {
                private final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun;

                private FluentBrokerAPIBuilder_Col1_NoExec_Doc1(final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun) {
                    this.documentLookupFun = documentLookupFun;
                }

                public <CDR> FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec<CDR> execute(final BiFunction<Collection, DocumentImpl, CDR> collectionDocumentOp) {
                    return new FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec<>(collectionDocumentOp);
                }

                public FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec withoutCollection() {
                    return new FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec();
                }

                public class FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec<CDR> {
                    private final BiFunction<Collection, DocumentImpl, CDR> collectionDocumentOp;

                    private FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec(final BiFunction<Collection, DocumentImpl, CDR> collectionDocumentOp) {
                        this.collectionDocumentOp = collectionDocumentOp;
                    }

                    public FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec withoutCollection() {
                        return new FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec();
                    }

                    public CDR doAll() throws PermissionDeniedException, LockException, EXistException {
                        final Tuple3<Optional<Void>, Optional<CDR>, Optional<Void>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.empty(), documentLookupFun, Optional.of(collectionDocumentOp), Optional.empty());
                        return result._2.get();
                    }

                    public class FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec {
                        private FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec() {}

                        public <DR> FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec_Exec<DR> execute(final Function<DocumentImpl, DR> documentOp) {
                            return new FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec_Exec<>(documentOp);
                        }
                    }

                    public class FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec_Exec<DR> {
                        private final Function<DocumentImpl, DR> documentOp;

                        private FluentBrokerAPIBuilder_Col1_NoExec_Doc1_Exec_NoExec_Exec(final Function<DocumentImpl, DR> documentOp) {
                            this.documentOp = documentOp;
                        }

                        public Tuple2<CDR, DR> doAll() throws PermissionDeniedException, LockException, EXistException {
                            final Tuple3<Optional<Void>, Optional<CDR>, Optional<DR>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.empty(), documentLookupFun, Optional.of(collectionDocumentOp), Optional.of(documentOp));
                            return new Tuple2<>(result._2.get(), result._3.get());
                        }
                    }
                }

                public class FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec {
                    private FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec() {}

                    public <DR> FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec_Exec<DR> execute(final Function<DocumentImpl, DR> documentOp) {
                        return new FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec_Exec<>(documentOp);
                    }

                    public class FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec_Exec<DR> {
                        private final Function<DocumentImpl, DR> documentOp;

                        private FluentBrokerAPIBuilder_Col1_NoExec_Doc1_NoExec_Exec(final Function<DocumentImpl, DR> documentOp) {
                            this.documentOp = documentOp;
                        }

                        public DR doAll() throws PermissionDeniedException, LockException, EXistException {
                            final Tuple3<Optional<Void>, Optional<Void>, Optional<DR>> result = FluentBrokerAPIBuilder.this.doAll(collectionUri, collectionLockMode, Optional.empty(), documentLookupFun, Optional.empty(), Optional.of(documentOp));
                            return result._3.get();
                        }
                    }
                }
            }
        }

        private <CR, CDR, DR> Tuple3<Optional<CR>, Optional<CDR>, Optional<DR>> doAll(
                final XmldbURI collectionUri, final LockMode collectionLockMode,
                final Optional<Function<Collection, CR>> collectionFun,
                @Nullable final Function<Collection, Tuple2<XmldbURI, LockMode>> documentLookupFun,
                final Optional<BiFunction<Collection, DocumentImpl, CDR>> collectionDocumentFun,
                final Optional<Function<DocumentImpl, DR>> documentFun) throws EXistException, PermissionDeniedException, LockException {

            final Optional<CR> collectionFunResult;
            final Optional<CDR> collectionDocumentFunResult;
            final Optional<DR> documentFunResult;

            try(final DBBroker broker = brokerPool.getBroker()) {

                Collection collection = null;
                try {
                    collection = broker.openCollection(collectionUri, collectionLockMode);

                    if(collection == null) {
                        throw new EXistException("No such Collection: " + collectionUri);
                    }

                    final Collection c = collection;    // needed final for closures
                    collectionFunResult = collectionFun.map(cf -> cf.apply(c));

                    if(collectionDocumentFun.isPresent() || documentFun.isPresent()) {

                        final Tuple2<XmldbURI, LockMode> docAccess = documentLookupFun.apply(collection);

                        DocumentImpl document = null;
                        try {
                            document = collection.getDocumentWithLock(broker, docAccess._1, docAccess._2);

                            final DocumentImpl d = document;

                            collectionDocumentFunResult = collectionDocumentFun.map(cdf -> cdf.apply(c, d));

                            // release the Collection lock early
                            collection.release(collectionLockMode);
                            collection = null;  // signal closed

                            documentFunResult = documentFun.map(df -> df.apply(d));

                        } finally {
                            if(document != null) {
                                document.getUpdateLock().release(docAccess._2);
                                document = null;
                            }
                        }
                    } else {
                        collectionDocumentFunResult = Optional.empty();
                        documentFunResult = Optional.empty();
                    }
                } finally {
                    // catch-all to close the collection in case of an exception and it hasn't been closed
                    if(collection != null) {
                        collection.release(collectionLockMode);
                        collection = null;
                    }
                }
            }

            return new Tuple3<>(collectionFunResult, collectionDocumentFunResult, documentFunResult);
        }



//        public class FluentBrokerAPIBuilder_ColN {
//            private final Tuple2<XmldbURI, LockMode> collectionsAndLockModes[];
//
//            private FluentBrokerAPIBuilder_ColN(final Tuple2<XmldbURI, LockMode>... collectionsAndLockModes) {
//                this.collectionsAndLockModes = collectionsAndLockModes;
//            }
//
//            public Object[] execute(final Function<Collection, Object>... collectionOps) {
//                if(collectionsAndLockModes.length != collectionOps.length) {
//                    throw new IllegalStateException();
//                }
//            }
//        }
    }
}
