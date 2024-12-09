package bio.guoda.preston.stream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URISyntaxException;

import static bio.guoda.preston.stream.ContentStreamHandlerImpl.wrapIRI;

public class ArchiveStreamHandler implements ContentStreamHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ArchiveStreamHandler.class);

    private ContentStreamHandler contentStreamHandler;

    public ArchiveStreamHandler(ContentStreamHandler contentStreamHandler) {
        this.contentStreamHandler = contentStreamHandler;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        Pair<ArchiveInputStream, String> archiveStreamAndFormat = getArchiveStreamAndFormat(is);
        if (archiveStreamAndFormat != null) {
            if (!ArchiveStreamFactory.CPIO.equals(archiveStreamAndFormat.getRight())) {
                handleArchiveEntries(version, archiveStreamAndFormat.getLeft(), archiveStreamAndFormat.getRight());
                return true;
            }
        }
        return false;
    }

    public static Pair<ArchiveInputStream, String> getArchiveStreamAndFormat(InputStream in) {
        try {
            String archiveFormat = ArchiveStreamFactory.detect(in);
            // do not close this stream; it would also close the "in" stream
            ArchiveInputStream archiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(in);
            return Pair.of(archiveInputStream, archiveFormat);
        } catch (ArchiveException e) {
            return null;
        }
    }

    private void handleArchiveEntries(IRI version, ArchiveInputStream in, String archiveFormat) throws ContentStreamException {
        ArchiveEntry entry = null;
        try {
            while (shouldKeepProcessing() && (entry = in.getNextEntry()) != null) {
                if (!entry.isDirectory() && in.canReadEntryData(entry)) {
                    IRI entryIri;
                    try {
                        entryIri = wrapIRI(archiveFormat, version, entry.getName());
                    } catch (URISyntaxException e) {
                        throw new ContentStreamException("failed to create content URI related to entry [" + entry.getName() + "] in [" + version.getIRIString() + "]", e);
                    }
                    if (shouldReadArchiveEntry(entryIri)) {
                        contentStreamHandler.handle(entryIri, CloseShieldInputStream.wrap(in));
                    }
                }
            }
        } catch (Throwable th) {
            String dataCoordinates = entry == null || entry.isDirectory()
                    ? version.getIRIString()
                    : "<" + archiveFormat + ":" + version.getIRIString() + "!/" + entry.getName() + ">";
            String msg = "failed to process " + dataCoordinates;
            LOG.warn(msg, th);
            throw new ContentStreamException(msg, th);
        }
    }

    protected boolean shouldReadArchiveEntry(IRI entryIri) {
        return true;
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }

}
