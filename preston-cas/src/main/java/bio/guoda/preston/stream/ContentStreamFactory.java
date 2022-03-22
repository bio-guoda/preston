package bio.guoda.preston.stream;

import bio.guoda.preston.store.HashKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.stream.ContentStreamUtil.cutBytes;

public class ContentStreamFactory implements InputStreamFactory {
    public static final String URI_PREFIX_CUT = "cut:";
    private final IRI targetIri;
    private final IRI contentReference;

    public static boolean hasMatchingGZipPrefix(IRI iri, IRI targetIri) {
        boolean prefixMatches = false;
        String iriString = iri.getIRIString();
        String targetIriString = targetIri.getIRIString();

        // turn gz:[something]!/bla.txt  (syntax used by Apache's Virtual Filesystem https://commons.apache.org/proper/commons-vfs/)
        // into gz:[something]
        //
        if (StringUtils.startsWith(targetIriString, "gz:")
                && StringUtils.startsWith(iriString, "gz:")
                && StringUtils.length(targetIriString) > StringUtils.length(iriString)) {
            prefixMatches = StringUtils.startsWith(targetIriString.substring(iriString.length()), "!/");
        }
        return prefixMatches;
    }


    public ContentStreamFactory(IRI iri) {
        this.targetIri = iri;
        this.contentReference = HashKeyUtil.extractContentHash(targetIri);
    }

    @Override
    public InputStream create(InputStream is) throws IOException {
        InputStream contentStream = requestContentStream(is);
        if (contentStream == null) {
            throw new IOException("failed to find content identified by [" + targetIri + "]");
        }
        return contentStream;
    }

    private InputStream requestContentStream(InputStream is) throws IOException {
        ContentStreamRequest streamRequest = new ContentStreamRequest();
        try {
            streamRequest.handle(contentReference, is);
        } catch (ContentStreamException e) {
            throw new IOException("failed create inputstream", e);
        }
        return streamRequest.getContentStream();
    }


    private class ContentStreamRequest implements ContentStreamHandler {

        private final ContentStreamHandler handler;
        private InputStream contentStream;
        private boolean keepReading = true;

        ContentStreamRequest() {
            this.handler = new ContentStreamHandlerImpl(
                    new ArchiveEntryStreamHandler(this, targetIri),
                    new CompressedStreamHandler(this),
                    new LineStreamHandler(this));
        }

        @Override
        public boolean handle(IRI iri, InputStream in) throws ContentStreamException {
            if (!shouldKeepReading()) {
                throw new ContentStreamException("request handler cannot be re-used");
            }

            Matcher nextOperatorMatcher = Pattern
                    .compile(String.format("([^:]+):%s", iri.getIRIString()))
                    .matcher(targetIri.getIRIString());

            if (iri.getIRIString().equals(targetIri.getIRIString())
                    || hasMatchingGZipPrefix(iri, targetIri)) {
                contentStream = in;
                stopReading();
                return true;
            } else if (nextOperatorMatcher.find()
                    && nextOperatorMatcher.group(1).equals("cut")) {
                cutAndParseBytes(iri, in);
                return true;
            } else {
                return handler.handle(iri, in);
            }
        }



        private void cutAndParseBytes(IRI iri, InputStream in) throws ContentStreamException {
            // do not support open-ended cuts, e.g. "b5-" or "b-5"
            Matcher byteRangeMatcher = Pattern
                    .compile(String.format("^" + URI_PREFIX_CUT + "%s!/b(?<first>[0-9]+)-(?<last>[0-9]+)$", iri.getIRIString()))
                    .matcher(targetIri.getIRIString());

            if (byteRangeMatcher.find()) {
                long firstByteIndex = Long.parseLong(byteRangeMatcher.group("first")) - 1;
                long lastByteIndex = Long.parseLong(byteRangeMatcher.group("last"));

                InputStream cutIs = null;
                try {
                    cutIs = cutBytes(in, firstByteIndex, lastByteIndex);
                } catch (IOException e) {
                    throw new ContentStreamException("failed to cut inputstream", e);
                }
                handle(
                        toIRI(byteRangeMatcher.group()),
                        cutIs
                );
            } else {
                throw new IllegalArgumentException("[" + iri + "] is not a valid cut URI");
            }
        }

        private void stopReading() {
            keepReading = false;
        }

        @Override
        public boolean shouldKeepReading() {
            return keepReading;
        }

        InputStream getContentStream() {
            return contentStream;
        }

    }
}
