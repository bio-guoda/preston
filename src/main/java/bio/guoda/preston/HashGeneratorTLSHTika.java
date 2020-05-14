package bio.guoda.preston;

import com.trendmicro.tlsh.Tlsh;
import com.trendmicro.tlsh.TlshCreator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public class HashGeneratorTLSHTika extends HashGeneratorTLSHashIRI {

    private static Tika tika = null;

    @Override
    public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        if (tika == null) {
            tika = getTika();
        }
        Reader reader = tika.parse(is);
        ReaderInputStream tikaIs = new ReaderInputStream(reader, StandardCharsets.UTF_8);
        String hexEncodedHash = calculateLTSH(tikaIs, os, shouldCloseInputStream);
        return Hasher.toHashIRI(HashType.tika_tlsh, StringUtils.substring(hexEncodedHash, 3));
    }

    public Tika getTika() throws IOException {
        // apply text extractor Apache Tika to handle zip files or other non-text formats
        TikaConfig config;
        try {
            config = new TikaConfig(getClass().getResourceAsStream("/bio/guoda/preston/tika-config.xml"));
        } catch (TikaException | SAXException e) {
            throw new IOException("failed to configure tika streaming");
        }

        return new Tika(config);
    }

    static String calculateLTSH(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        TlshCreator tlshCreator = new TlshCreator();

        InputStream input = new TLSHInputStream(is, tlshCreator);

        IOUtils.copy(input, os);
        if (!tlshCreator.isValid()) {
            throw new IOException("LTSH invalid, likely due to too little input or variance");
        }
        if (shouldCloseInputStream) {
            is.close();
        }
        Tlsh hash = tlshCreator.getHash();
        return StringUtils.lowerCase(hash.getEncoded());
    }

    private static class TLSHInputStream extends FilterInputStream {
        TlshCreator tlshCreator;

        TLSHInputStream(InputStream is, TlshCreator tlshCreator) {
            super(is);
            this.tlshCreator = tlshCreator;
        }

        @Override
        public int read() throws IOException {
            int read = super.read();
            if (read != -1) {
                tlshCreator.update(new byte[] { (byte)read }, 0, 1);
            }
            return read;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int numberOfBytesRead = super.read(b, off, len);
            if (numberOfBytesRead > -1) {
                tlshCreator.update(b, off, numberOfBytesRead);
            }
            return numberOfBytesRead;
        }
    }

}
