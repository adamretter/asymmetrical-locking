package swap;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Offers Asymmetrical Locking through lock swapping and try-with-resources.
 */
public class AsymmetricalLockingExample {

    public static void main(String args[]) throws Exception {

        try (final ManagedRelease<Collection> mcol =
                     new ManagedRelease<>(getCollection("col1 name", LockMode.WRITE_LOCK))) {

            // Here we do any operations that only require the Collection

            try (final ManagedRelease<Document> mdoc =
                         mcol.withAsymetrical(mcol.resource.getDocument("doc1 name", LockMode.WRITE_LOCK))) {

                // Here we do any operations that require both the Collection and Document (rare).

            }  // NOTE: Collection is released here

            // Here we do some operations on the document (of the Collection)

        }  // NOTE: Document is released here

    }

    private static class ManagedRelease<T extends AutoCloseable> implements AutoCloseable {
        final T resource;
        private Supplier<Optional<Exception>> closer;

        public ManagedRelease(final T resource) {
            this.resource = resource;
            this.closer = asCloserFn(resource);
        }

        private ManagedRelease(final T resource, final Supplier<Optional<Exception>> closer) {
            this.resource = resource;
            this.closer = closer;
        }

        public <U extends AutoCloseable> ManagedRelease<U> withAsymetrical(final U otherResource) {
            // switch the closers of ManagedRelease<T> and ManagedRelease<U>
            final ManagedRelease<U> asymManagedResource = new ManagedRelease<>(otherResource, closer);
            this.closer = asCloserFn(otherResource);
            return asymManagedResource;
        }

        @Override
        public void close() throws Exception {
            final Optional<Exception> maybeEx = closer.get();
            if(maybeEx.isPresent()) {
                throw maybeEx.get();
            }
        }

        private static Supplier<Optional<Exception>> asCloserFn(final AutoCloseable autoCloseable) {
            return () -> {
                try {
                    autoCloseable.close();
                    return Optional.empty();
                } catch (final Exception e) {
                    return Optional.of(e);
                }
            };
        }
    }

    private enum LockMode {
        WRITE_LOCK
    }

    private static class Document implements AutoCloseable {
        @Override
        public void close() throws Exception {
            System.out.println("Closing Document");
        }
    }

    private static class Collection implements AutoCloseable {

        Document getDocument(final String name, final LockMode lockMode) {
            System.out.println("Getting Document");
            return new Document();
        }

        @Override
        public void close() throws Exception {
            System.out.println("Closing Collection");
        }
    }

    private static Collection getCollection(final String name, final LockMode lockMode) {
        System.out.println("Getting Collection");
        return new Collection();
    }

}
