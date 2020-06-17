package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
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
        }
    }

    private void attemptToParseAsPublishers(Quad statement, IRI toBeParsed) {
        if (StringUtils.contains(statement.getSubject().ntriplesString(), PUBLISHERS_URI)) {
            ArrayList<Quad> nodes = new ArrayList<>();
            parsePublishers(toBeParsed, nodes::add);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

   private void attemptToParseAsRecordSets(Quad statement, IRI toBeParsed) {
        if (StringUtils.contains(statement.getSubject().ntriplesString(), RECORDSETS_URI)) {
            ArrayList<Quad> nodes = new ArrayList<>();
            parseRecordSets(toBeParsed, nodes::add);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    static void parsePublishers(IRI parent, StatementEmitter emitter, InputStream is) throws IOException {
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

    private static void parseRecordSets(IRI parent, StatementEmitter emitter, InputStream is) throws IOException {
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

    private static void verifyItemCount(JsonNode r, AtomicInteger itemCounter) {
        if (r.has("itemCount") && r.get("itemCount").isIntegralNumber()) {
            int itemCount = r.get("itemCount").asInt();
            if (itemCount != itemCounter.get()) {
                throw new IllegalArgumentException("paging not supported, but more pages are needed: got [" + itemCounter.get() +"], but expected [" + itemCount + "]");
            }
        }
    }

    private void parsePublishers(IRI refNode, StatementEmitter emitter) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parsePublishers(refNode, emitter, is);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse publishers [" + refNode.toString() + "]", e);
        }
    }

    private void parseRecordSets(IRI refNode, StatementEmitter emitter) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parseRecordSets(refNode, emitter, is);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse recordsets [" + refNode.toString() + "]", e);
        }
    }

}
