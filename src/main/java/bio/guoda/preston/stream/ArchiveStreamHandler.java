package bio.guoda.preston.stream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import static bio.guoda.preston.stream.ContentStreamHandlerImpl.wrapIRI;

public class ArchiveStreamHandler implements ContentStreamHandler {

    private ContentStreamHandler contentStreamHandler;

    public ArchiveStreamHandler(ContentStreamHandler contentStreamHandler) {
        this.contentStreamHandler = contentStreamHandler;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        Pair<ArchiveInputStream, String> archiveStreamAndFormat = getArchiveStreamAndFormat(is);
        if (archiveStreamAndFormat != null) {
            try {
                handleArchiveEntries(version, archiveStreamAndFormat.getLeft(), archiveStreamAndFormat.getRight());
            } catch (IOException e) {
                throw new ContentStreamException("failed to read [" + version + "]", e);
            }
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

    private void handleArchiveEntries(IRI version, ArchiveInputStream in, String archiveFormat) throws ContentStreamException, IOException {
        ArchiveEntry entry;
        while (shouldKeepReading() && (entry = in.getNextEntry()) != null) {
            if (in.canReadEntryData(entry)) {
                IRI entryIri;
                try {
                    entryIri = wrapIRI(archiveFormat, version, entry.getName());
                } catch (URISyntaxException e) {
                    throw new ContentStreamException("failed to create content URI", e);
                }
                if (shouldReadArchiveEntry(entryIri)) {
                    contentStreamHandler.handle(entryIri, in);
                }
            }
        }
    }

    protected boolean shouldReadArchiveEntry(IRI entryIri) {
        return true;
    }

    @Override
    public boolean shouldKeepReading() {
        return contentStreamHandler.shouldKeepReading();
    }

}
