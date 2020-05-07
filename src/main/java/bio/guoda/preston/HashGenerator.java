package bio.guoda.preston;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface HashGenerator<T> {
    T hash(InputStream is) throws IOException;
    T hash(InputStream is, OutputStream os) throws IOException;
    T hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException;
}


