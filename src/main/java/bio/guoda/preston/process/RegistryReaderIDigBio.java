package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.util.ResultPagerUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class RegistryReaderIDigBio extends ProcessorReadOnly {

    private final static Log LOG = LogFactory.getLog(RegistryReaderIDigBio.class);

    public static final String PUBLISHERS_URI = "https://search.idigbio.org/v2/search/publishers";
    public static final IRI IDIGBIO_PUBLISHER_REGISTRY = toIRI(URI.create(PUBLISHERS_URI + "?limit=10000"));

    public static final String RECORDSETS_URI = "https://search.idigbio.org/v2/search/recordsets";
    public static final IRI IDIGBIO_RECORDSETS_REGISTRY = toIRI(URI.create(RECORDSETS_URI + "?limit=10000"));

    public static final String RECORDS_URI = "https://search.idigbio.org/v2/search/records";

    public static final String MEDIA_RECORDS_URI = "https://search.idigbio.org/v2/view/mediarecords/";

    public static final Pattern SEARCH_API_IRI_MATCHER = Pattern.compile("(.*//)(search\\.)(.*)(/search/.*$)");

    public RegistryReaderIDigBio(BlobStoreReadOnly blobStore, StatementsListener listener) {
        super(blobStore, listener);
    }

    @Override
    public void on(Quad statement) {
        if (statement.getSubject().equals(Seeds.IDIGBIO)
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream<Quad> quadStream = Stream.of(toStatement(Seeds.IDIGBIO, IS_A, ORGANIZATION),
                    toStatement(IDIGBIO_PUBLISHER_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a registry of RSS Feeds that point to publishers of Darwin Core archives, and EML descriptors.")),
                    toStatement(IDIGBIO_PUBLISHER_REGISTRY, CREATED_BY, Seeds.IDIGBIO),
                    toStatement(IDIGBIO_PUBLISHER_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(IDIGBIO_PUBLISHER_REGISTRY, HAS_VERSION, toBlank()),

                    toStatement(IDIGBIO_RECORDSETS_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a registry of recordsets that describe Darwin Core archives, and EML descriptors.")),
                    toStatement(IDIGBIO_RECORDSETS_REGISTRY, CREATED_BY, Seeds.IDIGBIO),
                    toStatement(IDIGBIO_RECORDSETS_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(IDIGBIO_RECORDSETS_REGISTRY, HAS_VERSION, toBlank())
            );
            ActivityUtil.emitAsNewActivity(quadStream, this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)) {
            attemptToParseAsPublishers(statement, (IRI) getVersion(statement));
            attemptToParseAsRecordSets(statement, (IRI) getVersion(statement));
            attemptToParseAsRecords(statement, (IRI) getVersion(statement));
            attemptToParseAsMediaRecord(statement, (IRI) getVersion(statement));
        }
    }

    private void attemptToParseAsPublishers(Quad statement, IRI toBeParsed) {
        final BlankNodeOrIRI subject = statement.getSubject();
        if (isPublisherEndpoint(subject)) {
            ArrayList<Quad> nodes = new ArrayList<>();
            parsePublishers(toBeParsed, new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            });
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    private void attemptToParseAsRecordSets(Quad statement, IRI toBeParsed) {
        final BlankNodeOrIRI subject = statement.getSubject();
        if (isRecordSetEndpoint(subject)) {
            ArrayList<Quad> nodes = new ArrayList<>();
            parseRecordSets(toBeParsed, new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            });
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    public static boolean isMediaRecordEndpoint(BlankNodeOrIRI subject) {
        return StringUtils.contains(subject.ntriplesString(), MEDIA_RECORDS_URI);
    }

    public static boolean isPublisherEndpoint(BlankNodeOrIRI subject) {
        return StringUtils.contains(subject.ntriplesString(), PUBLISHERS_URI);
    }

    public static boolean isRecordSetEndpoint(BlankNodeOrIRI subject) {
        return StringUtils.contains(subject.ntriplesString(), RECORDSETS_URI);
    }

    public static boolean isRecordsEndpoint(BlankNodeOrIRI subject) {
        return StringUtils.contains(subject.ntriplesString(), RECORDS_URI)
                && !isRecordSetEndpoint(subject);
    }


    private void attemptToParseAsRecords(Quad statement, IRI resourceIRI) {
        final BlankNodeOrIRI subject = statement.getSubject();
        if (isRecordsEndpoint(subject)) {
            ArrayList<Quad> nodes = new ArrayList<>();
            parseRecords(resourceIRI, new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, (IRI) statement.getSubject());
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    private void attemptToParseAsMediaRecord(Quad statement, IRI resourceIRI) {
        final BlankNodeOrIRI subject = statement.getSubject();
        if (isMediaRecordEndpoint(subject)) {
            ArrayList<Quad> nodes = new ArrayList<>();
            parseMediaRecord(resourceIRI, new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, (IRI) subject);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }


    static void parsePublishers(IRI parent, StatementsEmitter emitter, InputStream is) throws IOException {
        AtomicInteger itemCounter = new AtomicInteger();
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                itemCounter.incrementAndGet();
                String publisherUUID = item.get("uuid").asText();
                IRI refNodePublisher = toIRI(publisherUUID);
                emitter.emit(toStatement(parent, HAD_MEMBER, refNodePublisher));
                JsonNode data = item.get("data");
                if (item.has("data")) {
                    String rssFeedUrl = data.has("rss_url") ? data.get("rss_url").asText() : null;
                    if (StringUtils.isNotBlank(rssFeedUrl)) {
                        IRI refNodeFeed = toIRI(rssFeedUrl);
                        emitter.emit(toStatement(refNodePublisher, HAD_MEMBER, refNodeFeed));
                        emitter.emit(toStatement(refNodeFeed, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_RSS)));
                        emitter.emit(toStatement(refNodeFeed, HAS_VERSION, toBlank()));
                    }
                }
            }
        }
        verifyItemCount(r, itemCounter);
    }

    private static void parseRecordSets(IRI parent, StatementsEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        AtomicInteger itemCounter = new AtomicInteger();

        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                itemCounter.incrementAndGet();
                String recordSetUUID = item.get("uuid").asText();
                IRI refNodeRecordSet = toIRI(recordSetUUID);
                emitter.emit(toStatement(parent, HAD_MEMBER, refNodeRecordSet));
                JsonNode data = item.get("data");
                if (item.has("data")) {
                    String emlURL = data.has("eml_link") ? data.get("eml_link").asText() : null;
                    if (StringUtils.isNotBlank(emlURL)) {
                        IRI refNodeFeed = toIRI(emlURL);
                        emitter.emit(toStatement(refNodeRecordSet, HAD_MEMBER, refNodeFeed));
                        emitter.emit(toStatement(refNodeFeed, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_EML)));
                        emitter.emit(toStatement(refNodeFeed, HAS_VERSION, toBlank()));
                    }
                    String dwcaURL = data.has("link") ? data.get("link").asText() : null;
                    if (StringUtils.isNotBlank(dwcaURL)) {
                        IRI refNodeFeed = toIRI(dwcaURL);
                        emitter.emit(toStatement(refNodeRecordSet, HAD_MEMBER, refNodeFeed));
                        emitter.emit(toStatement(refNodeFeed, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_DWCA)));
                        emitter.emit(toStatement(refNodeFeed, HAS_VERSION, toBlank()));
                    }
                }
            }
        }

        verifyItemCount(r, itemCounter);
    }

    static void parseRecords(IRI resourceIRI, StatementsEmitter emitter, InputStream is, IRI pageIRI) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        AtomicInteger itemCounter = new AtomicInteger();


        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                itemCounter.incrementAndGet();
                String recordUUID = item.get("uuid").asText();
                IRI recordIRI = toIRI(UUID.fromString(recordUUID));
                emitter.emit(toStatement(resourceIRI, HAD_MEMBER, recordIRI));
                JsonNode indexTerms = item.get("indexTerms");
                if (item.has("indexTerms")) {
                    String recordSetUUID = indexTerms.has("recordset") ? indexTerms.get("recordset").asText() : null;
                    if (StringUtils.isNotBlank(recordSetUUID)) {
                        emitter.emit(toStatement(toIRI(UUID.fromString(recordSetUUID)), HAD_MEMBER, recordIRI));
                    }

                    if (indexTerms.has("mediarecords")) {
                        final JsonNode mediarecords = indexTerms.get("mediarecords");
                        for (JsonNode mediarecord : mediarecords) {
                            if (mediarecord.isValueNode()) {
                                final UUID mediaUUID = UUID.fromString(mediarecord.asText());
                                emitter.emit(toStatement(recordIRI, HAD_MEMBER, toIRI(mediaUUID)));
                                IRI mediaRecordIRI = resolveMediaUUID(pageIRI, mediaUUID);
                                if (mediaRecordIRI != null) {
                                    emitter.emit(toStatement(mediaRecordIRI, HAS_VERSION, RefNodeFactory.toBlank()));
                                    emitter.emit(toStatement(resolveMediaThumbnail(pageIRI, mediaUUID), HAS_VERSION, RefNodeFactory.toBlank()));
                                    emitter.emit(toStatement(resolveMediaWebView(pageIRI, mediaUUID), HAS_VERSION, RefNodeFactory.toBlank()));
                                    emitter.emit(toStatement(resolveMediaFullSize(pageIRI, mediaUUID), HAS_VERSION, RefNodeFactory.toBlank()));
                                }
                            }
                        }
                    }
                }
            }
        }

        final Long recordsFound = itemCounter.longValue();
        if (recordsFound > 0 && r.has("itemCount")) {
            final JsonNode itemCount = r.get("itemCount");
            if (itemCount.isNumber()) {
                Long recordsTotal = itemCount.asLong();
                ResultPagerUtil.emitPageRequests(pageIRI, recordsTotal, recordsFound, emitter);
            }
        }
    }

    static void parseMediaRecord(IRI resourceIRI, StatementsEmitter emitter, InputStream is, IRI pageIRI) throws IOException {
        JsonNode item = new ObjectMapper().readTree(is);
        String recordUUID = item.get("uuid").asText();
        IRI recordIRI = toIRI(UUID.fromString(recordUUID));
        emitter.emit(toStatement(resourceIRI, HAD_MEMBER, recordIRI));
        JsonNode indexTerms = item.get("indexTerms");
        if (item.has("indexTerms")) {
            if (indexTerms.has("records")) {
                String accessURI = indexTerms.has("accessuri") ? indexTerms.get("accessuri").asText() : null;
                if (StringUtils.isNotBlank(accessURI)) {
                    emitter.emit(toStatement(toIRI(accessURI), HAS_VERSION, RefNodeFactory.toBlank()));
                    emitter.emit(toStatement(recordIRI, toIRI("http://rs.tdwg.org/ac/terms/accessURI"), toIRI(accessURI)));
                }

                JsonNode records = indexTerms.get("records");
                for (JsonNode record : records) {
                    String specimenRecordUUID = record.isValueNode() ? record.asText() : null;
                    if (StringUtils.isNotBlank(specimenRecordUUID)) {
                        emitter.emit(toStatement(toIRI(accessURI), toIRI("http://xmlns.com/foaf/0.1/depicts"), toIRI(specimenRecordUUID)));
                    }
                }
            }

            if (indexTerms.has("mediarecords")) {
                final JsonNode mediarecords = indexTerms.get("mediarecords");
                for (JsonNode mediarecord : mediarecords) {
                    if (mediarecord.isValueNode()) {
                        final UUID mediaUUID = UUID.fromString(mediarecord.asText());
                        emitter.emit(toStatement(recordIRI, HAD_MEMBER, toIRI(mediaUUID)));
                        IRI mediaRecordIRI = resolveMediaUUID(pageIRI, mediaUUID);
                        if (mediaRecordIRI != null) {
                            emitter.emit(toStatement(mediaRecordIRI, HAS_VERSION, RefNodeFactory.toBlank()));
                        }
                    }
                }
            }
        }

    }

    static IRI resolveMediaUUID(IRI pageIRI, UUID mediaRecordUUID) {
        final String iriString = pageIRI.getIRIString();
        final Matcher matcher = SEARCH_API_IRI_MATCHER.matcher(iriString);
        return matcher.find()
                ? RefNodeFactory.toIRI(matcher.group(1) + matcher.group(2) + matcher.group(3) + "/view/mediarecords/" + mediaRecordUUID.toString())
                : null;
    }

    static IRI resolveMediaThumbnail(IRI pageIRI, UUID mediaRecordUUID) {
        return resolveMediaURLOfSize(pageIRI, mediaRecordUUID, "thumbnail");
    }

    static IRI resolveMediaWebView(IRI pageIRI, UUID mediaRecordUUID) {
        return resolveMediaURLOfSize(pageIRI, mediaRecordUUID, "webview");
    }

    private static IRI resolveMediaURLOfSize(IRI pageIRI, UUID mediaRecordUUID, String sizeType) {
        final String iriString = pageIRI.getIRIString();
        final Matcher matcher = SEARCH_API_IRI_MATCHER.matcher(iriString);
        return matcher.find()
                ? RefNodeFactory.toIRI(matcher.group(1) + "api." + matcher.group(3) + "/media/" + mediaRecordUUID.toString() + "?size=" + sizeType)
                : null;
    }

    static IRI resolveMediaFullSize(IRI pageIRI, UUID mediaRecordUUID) {
        return resolveMediaURLOfSize(pageIRI, mediaRecordUUID, "fullsize");
    }

    private static void verifyItemCount(JsonNode r, AtomicInteger itemCounter) {
        if (r.has("itemCount") && r.get("itemCount").isIntegralNumber()) {
            int itemCount = r.get("itemCount").asInt();
            if (itemCount != itemCounter.get()) {
                throw new IllegalArgumentException("paging not supported, but more pages are needed: got [" + itemCounter.get() + "], but expected [" + itemCount + "]");
            }
        }
    }

    private void parsePublishers(IRI refNode, StatementsEmitter emitter) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parsePublishers(refNode, emitter, is);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse publishers [" + refNode.toString() + "]", e);
        }
    }

    private void parseRecordSets(IRI refNode, StatementsEmitter emitter) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parseRecordSets(refNode, emitter, is);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse recordsets [" + refNode.toString() + "]", e);
        }
    }

    private void parseRecords(IRI refNode, StatementsEmitter emitter, IRI pageIRI) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parseRecords(refNode, emitter, is, pageIRI);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse records [" + refNode.toString() + "]", e);
        }
    }

    private void parseMediaRecord(IRI refNode, StatementsEmitter emitter, IRI pageIRI) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parseMediaRecord(refNode, emitter, is, pageIRI);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse records [" + refNode.toString() + "]", e);
        }
    }

}
