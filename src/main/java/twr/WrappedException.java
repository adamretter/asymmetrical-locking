package twr;

public class WrappedException extends Exception {
    public WrappedException(final Exception e) {
        super(e);
    }
}
