package bio.guoda.preston.util;

import java.io.IOException;
import java.io.InputStream;

interface JekyllPageWriter {
    void writePages(InputStream is,
                    JekyllUtil.JekyllPageFactory factory,
                    JekyllUtil.RecordType pageType) throws IOException;
}
