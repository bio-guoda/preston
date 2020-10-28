package bio.guoda.preston.process;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

abstract public class TextReader {

    public void attemptToParse(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        if (!attemptToParseAsArchive(version, in, emitter) && !attemptToParseAsCompressed(version, in, emitter)) {
            attemptToParseAsText(version, in, emitter);
        }
    }

    private boolean attemptToParseAsArchive(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        ArchiveInputStream archiveStream = getArchiveStream(in);
        if (archiveStream != null) {
            parseAsArchive(version, archiveStream, emitter);
            return true;
        }
        return false;
    }

    private ArchiveInputStream getArchiveStream(InputStream in) {
        try {
            return new ArchiveStreamFactory()
                    .createArchiveInputStream(in);
        } catch (ArchiveException e) {
            return null;
        }
    }

    private boolean attemptToParseAsCompressed(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        InputStream compressedStream = getCompressedStream(in);
        if (compressedStream != null) {
            parseAsCompressed(version, compressedStream, emitter);
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

    private boolean attemptToParseAsText(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        Charset charset = new UniversalEncodingDetector().detect(in, new Metadata());
        if (charset != null) {
            parseAsText(version, in, emitter, charset);
            return true;
        }
        return false;
    }

    protected abstract void parseAsText(IRI version, InputStream in, StatementEmitter emitter, Charset charset) throws IOException;

    protected abstract void parseAsArchive(IRI version, ArchiveInputStream in, StatementEmitter emitter) throws IOException;

    protected void parseAsCompressed(IRI version, InputStream in, StatementEmitter emitter) throws IOException {
        attemptToParse(version, in, emitter);
    }
}
