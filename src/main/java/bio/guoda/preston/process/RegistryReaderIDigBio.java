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
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.fromUUID;
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
    public static final IRI IDIGBIO_REGISTRY = toIRI(URI.create(PUBLISHERS_URI));

    public RegistryReaderIDigBio(BlobStoreReadOnly blobStore, StatementListener listener) {
        super(blobStore, listener);
    }

    @Override
    public void on(Quad statement) {
        if (statement.getSubject().equals(Seeds.IDIGBIO)
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream.of(toStatement(Seeds.IDIGBIO, IS_A, ORGANIZATION),
                    toStatement(IDIGBIO_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a registry of RSS Feeds that point to publishers of Darwin Core archives, and EML descriptors.")),
                    toStatement(IDIGBIO_REGISTRY, CREATED_BY, Seeds.IDIGBIO),
                    toStatement(IDIGBIO_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(IDIGBIO_REGISTRY, HAS_VERSION, toBlank()))
                    .forEach(this::emit);
        } else if (hasVersionAvailable(statement)) {
            parse(statement, (IRI) getVersion(statement));
        }
    }

    public void parse(Quad statement, IRI toBeParsed) {
        if (statement.getSubject().equals(IDIGBIO_REGISTRY)) {
            parsePublishers(toBeParsed);
        }
    }

    static void parsePublishers(IRI parent, StatementEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                String publisherUUID = item.get("uuid").asText();
                IRI refNodePublisher = fromUUID(publisherUUID);
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
    }

    private void parsePublishers(IRI refNode) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parsePublishers(refNode, this, is);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse [" + refNode.toString() + "]", e);
        }
    }

}
