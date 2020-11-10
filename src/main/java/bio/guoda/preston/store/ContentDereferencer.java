package bio.guoda.preston.store;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.ContentReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;

public class ContentDereferencer extends ContentReader implements Dereferencer<InputStream> {

    private final BlobStoreReadOnly blobStore;
    private String targetIriString;

    private IRI contentHash;

    public ContentDereferencer(BlobStoreReadOnly blobStore) {
        this.blobStore = blobStore;
    }

    private static InputStream cutBytes(InputStream in, long firstByteIndex, long lastByteIndex) throws IOException {
        IOUtils.skipFully(in, firstByteIndex);
        return new BoundedInputStream(in, (lastByteIndex - firstByteIndex));
    }

    @Override
    public InputStream dereference(IRI uri) throws IOException {
        targetIriString = uri.getIRIString();

        final Pattern contentHashPattern = Pattern.compile("hash://sha256/[a-fA-F0-9]{64}");
        Matcher matchHash = contentHashPattern.matcher(targetIriString);
        if (matchHash.find()) {
            contentHash = toIRI(matchHash.group());
        }

        try (InputStream is = blobStore.get(contentHash)) {
            return new ContentExtractor().getContentStream(contentHash, is);
        } catch (IOException | URISyntaxException e) {
            throw new IOException("failed to resolve [\" + contentHash.getIRIString() + \"]", e);
        }
    }

    private class ContentExtractor extends ContentReader {
        private InputStream contentStream;

        public InputStream getContentStream(IRI version, InputStream in) throws IOException, URISyntaxException {
            contentStream = null;
            attemptToParse(version, in);

            if (contentStream == null)
                throw new IOException();
            else
                return contentStream;
        }

        @Override
        public void attemptToParse(IRI version, InputStream in) throws IOException, URISyntaxException {
            Matcher nextOperatorMatcher = Pattern.compile(String.format("([^:]+):%s", version.getIRIString())).matcher(targetIriString);

            if (version.getIRIString().equals(targetIriString)) {
                contentStream = in;
                stopReading();
            }
            else if (nextOperatorMatcher.find() && nextOperatorMatcher.group(1).equals("cut")) {
                cutAndParseBytes(version, in);
            }
            else {
                super.attemptToParse(version, in);
            }
        }

        private void cutAndParseBytes(IRI version, InputStream in) throws IOException, URISyntaxException {
            // do not support open-ended cuts, e.g. "b5-" or "b-5"
            Matcher byteRangeMatcher = Pattern.compile(String.format("^cut:%s!/b(?<first>[0-9]+)-(?<last>[0-9]+)$", version.getIRIString())).matcher(targetIriString);
            if (byteRangeMatcher.find()) {
                long firstByteIndex = Long.parseLong(byteRangeMatcher.group("first")) - 1;
                long lastByteIndex = Long.parseLong(byteRangeMatcher.group("last"));

                attemptToParse(
                        toIRI(byteRangeMatcher.group()),
                        cutBytes(in, firstByteIndex, lastByteIndex)
                );
            }
            else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        protected boolean shouldReadArchiveEntry(IRI entryIri) {
            return isPartOfTargetIri(entryIri);
        }

        private boolean isPartOfTargetIri(IRI version) {
            return targetIriString.contains(version.getIRIString());
        }
    }
}
