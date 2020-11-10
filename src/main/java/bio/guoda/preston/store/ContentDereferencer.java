package bio.guoda.preston.store;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.ContentReader;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.util.ByteStreamUtil.cutBytes;

public class ContentDereferencer extends ContentReader implements Dereferencer<InputStream> {

    private final BlobStoreReadOnly blobStore;

    public ContentDereferencer(BlobStoreReadOnly blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public InputStream dereference(IRI iri) throws IOException {
        try {
            IRI contentHash = extractContentHash(iri);
            InputStream in = blobStore.get(contentHash);
            return new ContentExtractor(iri).getContentStream(in);
        } catch (IOException | URISyntaxException | IllegalArgumentException e) {
            throw new IOException("failed to dereference [" + iri.getIRIString() + "]", e);
        }
    }

    protected static IRI extractContentHash(IRI iri) throws IllegalArgumentException {
        final Pattern contentHashPattern = ValidatingKeyValueStreamSHA256IRI.SHA_256_PATTERN;
        Matcher contentHashMatcher = contentHashPattern.matcher(iri.getIRIString());

        IRI contentHash = (contentHashMatcher.find()) ? toIRI(contentHashMatcher.group()) : null;
        if (contentHash == null) {
            throw new IllegalArgumentException("[" + iri.getIRIString() + "] is not a content-based URI (e.g. \"...hash://abc123...\"");
        }
        else {
            return contentHash;
        }
    }

    private static class ContentExtractor extends ContentReader {
        private final IRI targetIri;
        private InputStream contentStream;

        public ContentExtractor(IRI iri) {
            this.targetIri = iri;
        }

        public InputStream getContentStream(InputStream in) throws IOException, URISyntaxException, IllegalArgumentException {
            IRI contentReference = extractContentHash(targetIri);
            contentStream = null;
            attemptToParse(contentReference, in);

            if (contentStream == null)
                throw new IOException();
            else
                return contentStream;
        }

        @Override
        public void attemptToParse(IRI iri, InputStream in) throws IOException, URISyntaxException {
            Matcher nextOperatorMatcher = Pattern
                    .compile(String.format("([^:]+):%s", iri.getIRIString()))
                    .matcher(targetIri.getIRIString());

            if (iri.getIRIString().equals(targetIri.getIRIString())) {
                contentStream = in;
                stopReading();
            }
            else if (nextOperatorMatcher.find() && nextOperatorMatcher.group(1).equals("cut")) {
                cutAndParseBytes(iri, in);
            }
            else {
                super.attemptToParse(iri, in);
            }
        }

        private void cutAndParseBytes(IRI iri, InputStream in) throws IOException, URISyntaxException {
            // do not support open-ended cuts, e.g. "b5-" or "b-5"
            Matcher byteRangeMatcher = Pattern
                    .compile(String.format("^cut:%s!/b(?<first>[0-9]+)-(?<last>[0-9]+)$", iri.getIRIString()))
                    .matcher(targetIri.getIRIString());

            if (byteRangeMatcher.find()) {
                long firstByteIndex = Long.parseLong(byteRangeMatcher.group("first")) - 1;
                long lastByteIndex = Long.parseLong(byteRangeMatcher.group("last"));

                attemptToParse(
                        toIRI(byteRangeMatcher.group()),
                        cutBytes(in, firstByteIndex, lastByteIndex)
                );
            }
            else {
                throw new IllegalArgumentException("[" + iri + "] is not a valid cut URI");
            }
        }

        @Override
        protected boolean shouldReadArchiveEntry(IRI entryIri) {
            return isPartOfTargetIri(entryIri);
        }

        private boolean isPartOfTargetIri(IRI iri) {
            return targetIri.getIRIString().contains(iri.getIRIString());
        }
    }

    private static class ContentUriException extends Exception {
        public ContentUriException(String message) {
            super(message);
        }

        public ContentUriException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
