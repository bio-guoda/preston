package bio.guoda.preston.process;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;

abstract public class TextReader {

    public void attemptToParse(IRI version, InputStream in) throws IOException, URISyntaxException {
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
            // do not close this stream; it would also close the "in" stream
            ArchiveInputStream archiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(in);
            return Pair.of(archiveInputStream, archiveFormat);
        } catch (ArchiveException e) {
            return null;
        }
    }

    private boolean attemptToParseAsCompressed(IRI version, InputStream in) throws IOException, URISyntaxException {
        Pair<CompressorInputStream, String> compressedStreamAndFormat = getCompressedStreamAndFormat(in);
        if (compressedStreamAndFormat != null) {
            parseAsCompressed(version, compressedStreamAndFormat.getLeft(), compressedStreamAndFormat.getRight());
            return true;
        }
        return false;
    }

    private Pair<CompressorInputStream, String> getCompressedStreamAndFormat(InputStream in) {
        try {
            String compressionFormat = CompressorStreamFactory.detect(in);
            // do not close this stream; it would also close the "in" stream
            CompressorInputStream compressedInputStream = new CompressorStreamFactory()
                    .createCompressorInputStream(in);
            return Pair.of(compressedInputStream, compressionFormat);
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

    protected void parseAsArchive(IRI version, ArchiveInputStream in, String archiveFormat) throws IOException {
        ArchiveEntry entry;
        while ((entry = in.getNextEntry()) != null) {
            if (in.canReadEntryData(entry)) {
                // do not close this stream; it would also close the "in" stream
                InputStream markableEntryStream = new BufferedInputStream(in);
                try {
                    attemptToParse(getWrappedIri(archiveFormat, version, entry.getName()), markableEntryStream);
                } catch (IOException | URISyntaxException e) {
                    // ignore; this is opportunistic
                }
            }
        }
    }

    protected void parseAsCompressed(IRI version, InputStream in, String compressionFormat) throws IOException, URISyntaxException {
        // do not close this stream; it would also close the "in" stream
        InputStream markableStream = new BufferedInputStream(in);
        attemptToParse(getWrappedIri(compressionFormat, version), markableStream);
    }

    public static IRI getWrappedIri(String prefix, IRI version, String suffix) throws URISyntaxException {
        URI uri = new URI(prefix, version.getIRIString() + (suffix != null ? "!/" + suffix : ""), null);
        return toIRI(uri);
    }

    public static IRI getWrappedIri(String prefix, IRI version) throws URISyntaxException {
        return getWrappedIri(prefix, version, null);
    }
}
