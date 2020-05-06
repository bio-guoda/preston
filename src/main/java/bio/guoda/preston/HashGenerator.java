package bio.guoda.preston;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface HashGenerator {
    String hash(InputStream is) throws IOException;
    String hash(InputStream is, OutputStream os) throws IOException;
    String hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException;
}


