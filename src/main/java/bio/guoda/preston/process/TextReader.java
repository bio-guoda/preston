package bio.guoda.preston.process;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

abstract public class TextReader {

    public void attemptToParse(IRI version, InputStream in) throws IOException {
        if (!attemptToParseAsArchive(version, in) && !attemptToParseAsCompressed(version, in)) {
            attemptToParseAsText(version, in);
        }
    }

    private boolean attemptToParseAsArchive(IRI version, InputStream in) throws IOException {
        Pair<ArchiveInputStream, String> archiveStreamAndFormat = getArchiveStreamAndFormat(in);
        if (archiveStreamAndFormat != null) {
            parseAsArchive(version, archiveStreamAndFormat.getLeft(), archiveStreamAndFormat.getRight());
            return true;
        }
        return false;
    }

    private Pair<ArchiveInputStream, String> getArchiveStreamAndFormat(InputStream in) {
        try {
            String archiveFormat = ArchiveStreamFactory.detect(in);
            ArchiveInputStream archiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(archiveFormat, in);
            return Pair.of(archiveInputStream, archiveFormat);
        } catch (ArchiveException e) {
            return null;
        }
    }

    private boolean attemptToParseAsCompressed(IRI version, InputStream in) throws IOException {
        InputStream compressedStream = getCompressedStream(in);
        if (compressedStream != null) {
            parseAsCompressed(version, compressedStream);
            return true;
        }
        return false;
    }

    private InputStream getCompressedStream(InputStream in) {
        try {
            return new CompressorStreamFactory()
                    .createCompressorInputStream(in);
        } catch (CompressorException e) {
            return null;
        }
    }

    private boolean attemptToParseAsText(IRI version, InputStream in) throws IOException {
        Charset charset = new UniversalEncodingDetector().detect(in, new Metadata());
        if (charset != null) {
            parseAsText(version, in, charset);
            return true;
        }
        return false;
    }

    protected abstract void parseAsText(IRI version, InputStream in, Charset charset) throws IOException;

    protected abstract void parseAsArchive(IRI version, ArchiveInputStream in, String archiveFormat) throws IOException;

    protected void parseAsCompressed(IRI version, InputStream in) throws IOException {
        attemptToParse(version, in);
    }
}
