package bio.guoda.preston.cmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CopyShop {

    void copy(final InputStream is, final OutputStream os) throws IOException;
}
