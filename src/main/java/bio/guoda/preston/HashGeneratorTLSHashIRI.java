package bio.guoda.preston;

import com.trendmicro.tlsh.Tlsh;
import com.trendmicro.tlsh.TlshCreator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.Tika;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public class HashGeneratorTLSHashIRI implements HashGenerator<IRI> {
    @Override
    public IRI hash(InputStream is) throws IOException {
        return hash(is, new NullOutputStream());
    }

    @Override
    public IRI hash(InputStream is, OutputStream os) throws IOException {
        return hash(is, os, true);
    }

    @Override
    public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        String hexEncodedHash = calculateLTSH(is, os, shouldCloseInputStream);
        return Hasher.toHashIRI(HashType.TLSH, StringUtils.substring(hexEncodedHash, 3));
    }

    static String calculateLTSH(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        TlshCreator tlshCreator = new TlshCreator();

        // apply universal text extractor Apache Tika to handle zip files
        Reader reader = new Tika().parse(is);
        ReaderInputStream readerInputStream = new ReaderInputStream(reader, StandardCharsets.UTF_8);
        InputStream input = new TLSHInputStream(readerInputStream, tlshCreator);

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
