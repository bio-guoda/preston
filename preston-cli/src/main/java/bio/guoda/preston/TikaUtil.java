package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public class TikaUtil {

    public static Tika getTika() throws IOException {
        // apply text extractor Apache Tika to handle zip files or other non-text formats
        TikaConfig config;
        try {
            config = new TikaConfig(TikaUtil.class.getResourceAsStream("/bio/guoda/preston/tika-config.xml"));
        } catch (TikaException | SAXException e) {
            throw new IOException("failed to configure tika streaming");
        }

        return new Tika(config);
    }

    public static void copyText(InputStream in, OutputStream out) throws IOException {
        final Tika tika = getTika();
        final Reader parse = tika.parse(new BufferedInputStream(in));
        IOUtils.copy(parse, out, StandardCharsets.UTF_8);
    }
}
