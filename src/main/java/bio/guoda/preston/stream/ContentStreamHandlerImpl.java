package bio.guoda.preston.stream;

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

abstract public class ContentStreamHandlerImpl implements ContentStreamHandler {

    private boolean keepReading = true;

    @Override
    public boolean handle(IRI version, InputStream in) throws ContentStreamException {
        InputStream markableInputStream = (in.markSupported()) ? in : new BufferedInputStream(in);
        if (!attemptToParseAsArchive(version, markableInputStream)
                && !attemptToParseAsCompressed(version, markableInputStream)
                && !attemptToParseAsText(version, markableInputStream)) {
            // fail silently
        }
        return true;
    }

    private boolean attemptToParseAsArchive(IRI version, InputStream is) throws ContentStreamException {
        return new ContentStreamHandler() {

            @Override
            public boolean handle(IRI version, InputStream is) throws ContentStreamException {
                Pair<ArchiveInputStream, String> archiveStreamAndFormat = getArchiveStreamAndFormat(is);
                if (archiveStreamAndFormat != null) {
                    try {
                        parseAsArchive(version, archiveStreamAndFormat.getLeft(), archiveStreamAndFormat.getRight());
                    } catch (IOException e) {
                        throw new ContentStreamException("failed to read [" + version + "]", e);
                    }
                    return true;
                }
                return false;
            }
        }.handle(version, is);
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

    private boolean attemptToParseAsCompressed(IRI version, InputStream in) throws ContentStreamException {
        return new ContentStreamHandler() {

            @Override
            public boolean handle(IRI version, InputStream in) throws ContentStreamException {
                Pair<CompressorInputStream, String> compressedStreamAndFormat = getCompressedStreamAndFormat(in);
                if (compressedStreamAndFormat != null) {
                    parseAsCompressed(version, compressedStreamAndFormat.getLeft(), compressedStreamAndFormat.getRight());
                    return true;
                }
                return false;
            }
        }.handle(version, in);
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

    private boolean attemptToParseAsText(IRI version, InputStream is) throws ContentStreamException {
        return new TextStreamHandler().handle(version, is);
    }

    protected void parseAsText(IRI version, InputStream in, Charset charset) throws IOException {
    }

    protected void parseAsArchive(IRI version, ArchiveInputStream in, String archiveFormat) throws ContentStreamException, IOException {
        ArchiveEntry entry;
        while (shouldKeepReading() && (entry = in.getNextEntry()) != null) {
            if (in.canReadEntryData(entry)) {
                IRI entryIri = null;
                try {
                    entryIri = wrapIRI(archiveFormat, version, entry.getName());
                } catch (URISyntaxException e) {
                    throw new ContentStreamException("failed to create content URI", e);
                }
                if (shouldReadArchiveEntry(entryIri)) {
                    handle(entryIri, in);
                }
            }
        }
    }

    protected boolean shouldReadArchiveEntry(IRI entryIri) {
        return true;
    }

    protected void parseAsCompressed(IRI version, InputStream in, String compressionFormat) throws ContentStreamException {
        try {
            handle(wrapIRI(compressionFormat, version), in);
        } catch (URISyntaxException e) {
            throw new ContentStreamException("failed to create content URI", e);
        }
    }

    public static IRI wrapIRI(String prefix, IRI version, String suffix) throws URISyntaxException {
        URI uri = new URI(prefix, version.getIRIString() + (suffix != null ? "!/" + suffix : ""), null);
        return toIRI(uri);
    }

    public static IRI wrapIRI(String prefix, IRI version) throws URISyntaxException {
        return wrapIRI(prefix, version, null);
    }

    protected void stopReading() {
        setKeepReading(false);
    }

    protected void setKeepReading(boolean keepReading) {
        this.keepReading = keepReading;
    }

    protected boolean shouldKeepReading() {
        return keepReading;
    }

    public class TextStreamHandler implements ContentStreamHandler {

        @Override
        public boolean handle(IRI version, InputStream is) throws ContentStreamException {
            Charset charset = null;
            try {
                charset = new UniversalEncodingDetector().detect(is, new Metadata());
            } catch (IOException e) {
                throw new ContentStreamException("failed to detect charset", e);
            }
            if (charset != null) {
                try {
                    parseAsText(version, is, charset);
                } catch (IOException e) {
                    throw new ContentStreamException("failed to parse text", e);
                }
                return true;
            }
            return false;
        }
    }
}
