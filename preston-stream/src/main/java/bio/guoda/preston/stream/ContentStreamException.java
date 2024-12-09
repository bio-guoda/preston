package bio.guoda.preston.stream;

public class ContentStreamException extends Exception {

    public ContentStreamException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public ContentStreamException(String msg) {
        super(msg);
    }
}
