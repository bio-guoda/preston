package bio.guoda.preston.stream;

import bio.guoda.preston.process.PDFUtil;
import bio.guoda.preston.process.PageSelected;
import bio.guoda.preston.store.HashKeyUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.stream.ContentStreamUtil.cutBytes;
import static bio.guoda.preston.stream.ContentStreamUtil.getMarkSupportedInputStream;
import static bio.guoda.preston.stream.ContentStreamUtil.getMarkSupportedReader;

public class ContentStreamFactory implements InputStreamFactory {
    public static final String URI_PREFIX_CUT = "cut";
    public static final String URI_PREFIX_LINE = "line";
    public static final String URI_PREFIX_THUMBNAIL = "thumbnail";
    public static final String URI_PREFIX_PAGE = "pdf";
    private final IRI targetIri;
    private final IRI contentReference;

    public static boolean hasSupportedCompressionPrefix(IRI iri, IRI targetIri) {
        return hasMatchingPrefix(iri, targetIri, "gz:")
                || hasMatchingPrefix(iri, targetIri, "bzip2:");
    }

    public static boolean hasMatchingPrefix(IRI iri, IRI targetIri, String prefix) {
        boolean prefixMatches = false;
        String iriString = iri.getIRIString();
        String targetIriString = targetIri.getIRIString();

        // turn gz:[something]!/bla.txt  (syntax used by Apache's Virtual Filesystem https://commons.apache.org/proper/commons-vfs/)
        // into gz:[something]
        //
        if (StringUtils.startsWith(targetIriString, prefix)
                && StringUtils.startsWith(iriString, prefix)
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
        if (is == null) {
            throw new IOException("cannot find content identified by [" + targetIri + "]");
        }

        InputStream contentStream = requestContentStream(is);
        if (contentStream == null) {
            throw new IOException("cannot find content identified by [" + targetIri + "]");
        }
        return contentStream;
    }

    private InputStream requestContentStream(InputStream is) throws IOException {
        ContentStreamRequest streamRequest = new ContentStreamRequest();
        try {
            streamRequest.handle(contentReference, is);
        } catch (ContentStreamException e) {
            throw new IOException("failed to create inputstream for [" + contentReference.getIRIString() + "]", e);
        }
        return streamRequest.getContentStream();
    }

    private class ContentStreamRequest implements ContentStreamHandler {

        public static final String GROUPNAME_PAGE_NUMBER = "pageNumber";
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
            if (!shouldKeepProcessing()) {
                throw new ContentStreamException("request handler cannot be re-used");
            }

            Matcher nextOperatorMatcher = Pattern
                    .compile(String.format("([^:]+):%s(!/[^//]*)?", iri.getIRIString()))
                    .matcher(targetIri.getIRIString());

            if (iri.getIRIString().equals(targetIri.getIRIString())
                    || hasSupportedCompressionPrefix(iri, targetIri)) {
                contentStream = in;
                stopReading();
                return true;
            } else if (nextOperatorMatcher.find()) {
                if (nextOperatorMatcher.group(1).equals(URI_PREFIX_CUT)) {
                    cutAndParseBytes(iri, in);
                    return true;
                } else if (nextOperatorMatcher.group(1).equals(URI_PREFIX_LINE) && lineQueryIsComplex(iri)) {
                    handle(toIRI(nextOperatorMatcher.group()), createInputStreamForSelectedLines(iri, in));
                    return true;
                } else if (nextOperatorMatcher.group(1).equals(URI_PREFIX_THUMBNAIL)) {
                    handle(toIRI(nextOperatorMatcher.group()), createThumbnailForImageStream(iri, in));
                    return true;
                } else if (nextOperatorMatcher.group(1).equals(URI_PREFIX_PAGE)) {
                    handle(toIRI(nextOperatorMatcher.group()), selectPagesFromPDF(iri, in));
                    return true;
                }
            }

            return handler.handle(iri, in);
        }


        private InputStream createInputStreamForSelectedLines(IRI iri, InputStream in) throws ContentStreamException {
            InputStream markableIn = getMarkSupportedInputStream(in);
            Charset charset;
            try {
                charset = new UniversalEncodingDetector().detect(markableIn, new Metadata());
            } catch (IOException e) {
                throw new ContentStreamException("failed to detect charset", e);
            }

            SelectedLinesReader lineReader = new SelectedLinesReader(getLineNumberStream(iri, RangeType.Line, targetIri).iterator(),
                    getMarkSupportedReader(new InputStreamReader(markableIn, charset)));
            return new ReaderInputStream(lineReader, charset);
        }

        private InputStream createThumbnailForImageStream(IRI iri, InputStream in) throws ContentStreamException {
            try {
                if (in == null) {
                    throw new ContentStreamException("no content provided for [" + iri + "]");
                }
                BufferedImage srcImage = ImageIO.read(in);
                if (srcImage == null) {
                    throw new ContentStreamException("image cannot be read from [" + iri + "]");
                }
                BufferedImage scaledImage = Scalr.resize(srcImage, 256);
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    ImageIO.write(scaledImage, "jpg", outputStream);
                    return new ByteArrayInputStream(outputStream.toByteArray());
                }
            } catch (IOException e) {
                throw new ContentStreamException("failed to create thumbnail from [" + iri + "]");
            }
        }

        private InputStream selectPagesFromPDF(IRI iri, InputStream in) throws ContentStreamException {
            try {
                if (in == null) {
                    throw new ContentStreamException("no content provided for [" + iri + "]");
                }

                LongStream pageNumberStream = getLineNumberStream(iri, RangeType.Page, targetIri);

                Matcher pageMatcher = Pattern
                        .compile(String.format("%s:%s!/p(?<" + GROUPNAME_PAGE_NUMBER + ">[1-9IVXC][0-9IVXC]*)", URI_PREFIX_PAGE, iri.getIRIString()))
                        .matcher(targetIri.getIRIString());

                if (pageMatcher.matches()) {
                    try (ByteArrayOutputStream pdfOS = new ByteArrayOutputStream()) {
                        IOUtils.copy(in, pdfOS);
                        PDDocument doc = Loader.loadPDF(pdfOS.toByteArray());

                        PrimitiveIterator.OfLong iterator = pageNumberStream.iterator();

                        List<PageSelected> selectedPages = new ArrayList<>();
                        while (iterator.hasNext()) {
                            Long pageNumber = iterator.next();
                            selectedPages.add(PDFUtil.selectPage(Long.toString(pageNumber), doc));
                        }
                        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                            PDFUtil.saveAsPDF(selectedPages, targetIri, os);
                            return new ByteArrayInputStream(os.toByteArray());
                        }

                    }
                }
                throw new ContentStreamException("invalid pdf iri: [" + iri + "]");
            } catch (IOException e) {
                throw new ContentStreamException("failed to create pdf from [" + iri + "]");
            }
        }


        private boolean lineQueryIsComplex(IRI iri) {
            return getLineNumberStream(iri, RangeType.Line, targetIri).limit(2).count() > 1;
        }

        private void cutAndParseBytes(IRI iri, InputStream in) throws ContentStreamException {
            // do not support open-ended cuts, e.g. "b5-" or "b-5"
            Matcher byteRangeMatcher = Pattern
                    .compile(String.format("^%s:%s!/b(?<first>[0-9]+)-(?<last>[0-9]+)$", URI_PREFIX_CUT, iri.getIRIString()))
                    .matcher(targetIri.getIRIString());

            if (byteRangeMatcher.find()) {
                long firstByteIndex = Long.parseLong(byteRangeMatcher.group("first")) - 1;
                long lastByteIndex = Long.parseLong(byteRangeMatcher.group("last"));

                InputStream cutIs;
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
        public boolean shouldKeepProcessing() {
            return keepReading;
        }

        InputStream getContentStream() {
            return contentStream;
        }

    }

    public static LongStream getLineNumberStream(IRI iri, RangeType rangeType, IRI targetIri) {
        Matcher lineQueryMatcher = Pattern
                .compile(String.format("%s:%s!/([%s%s\\-,]*)", rangeType.getIriPrefix(), iri.getIRIString(), rangeType.getPrefix(), rangeType.getPattern()))
                .matcher(targetIri.getIRIString());

        if (lineQueryMatcher.find()) {
            String linesQuery = lineQueryMatcher.group(1);

            return Arrays.stream(linesQuery.split(","))
                    .flatMapToLong(lineRange -> {
                        final Pattern lineRangePattern = Pattern.compile(String.format("%s([%s]+)(?:-%s([%s]+))?", rangeType.getPrefix(), rangeType.getPattern(), rangeType.getPrefix(), rangeType.getPattern()));
                        Matcher lineRangeMatcher = lineRangePattern.matcher(lineRange);

                        if (lineRangeMatcher.find()) {
                            long firstLine = Long.parseLong(lineRangeMatcher.group(1));
                            long lastLine = lineRangeMatcher.group(2) == null ? firstLine : Long.parseLong(lineRangeMatcher.group(2));
                            return LongStream.rangeClosed(firstLine, lastLine);
                        } else {
                            return LongStream.empty();
                        }
                    });
        } else {
            throw new IllegalArgumentException("[" + iri + "] is not a valid line URI");
        }
    }

}
