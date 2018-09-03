package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.MimeTypes;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.cmd.CrawlContext;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static org.globalbioticinteractions.preston.RefNodeConstants.*;
import static org.globalbioticinteractions.preston.RefNodeConstants.CREATED_BY;
import static org.globalbioticinteractions.preston.RefNodeConstants.DESCRIPTION;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAD_MEMBER;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_FORMAT;
import static org.globalbioticinteractions.preston.RefNodeConstants.IS_A;
import static org.globalbioticinteractions.preston.RefNodeConstants.ORGANIZATION;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.*;

public class RegistryReaderIDigBio extends ProcessorReadOnly {

    private final static Log LOG = LogFactory.getLog(RegistryReaderIDigBio.class);
    public static final String PUBLISHERS_URI = "https://search.idigbio.org/v2/search/publishers";
    public static final IRI IDIGBIO_REGISTRY = toIRI(URI.create(PUBLISHERS_URI));

    public RegistryReaderIDigBio(BlobStoreReadOnly blobStore, CrawlContext context, StatementListener listener) {
        super(blobStore, context, listener);
    }

    @Override
    public void on(Triple statement) {
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

    public void parse(Triple statement, IRI toBeParsed) {
        if (statement.getObject().equals(IDIGBIO_REGISTRY)) {
            parsePublishers(toBeParsed);
        } else {
            parse(toBeParsed);
        }
    }


    private void parse(IRI iri) {
        try {
            // first parse document to check whether it is valid
            new XmlMapper().readTree(get(iri));
            /// then parse
            parseRssFeed(iri, this, get(iri));
        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
            // ignore - opportunistic parsing attempt
        }
    }

    static void parseRssFeed(final IRI parent1, StatementEmitter emitter, InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {

        XPathHandler handler = new XPathHandler() {
            @Override
            public void evaluateXPath(StatementEmitter emitter, NodeList nodeList) throws XPathExpressionException {
                for (int i = 0; i < nodeList.getLength(); i++) {
                    boolean isDWCA = false;
                    URI archiveURI = null;
                    URI emlURI = null;
                    UUID uuid = null;
                    Node item = nodeList.item(i);
                    NodeList childNodes = item.getChildNodes();
                    for (int j = 0; j < childNodes.getLength(); j++) {
                        Node child = childNodes.item(j);
                        String itemName = child.getNodeName();
                        String itemValue = child.getTextContent();

                        if ("guid".equals(itemName)) {
                            try {
                                uuid = UUID.fromString(itemValue);
                            } catch (IllegalArgumentException ex) {
                                // ignore
                            }
                        } else if (Arrays.asList("ipt_eml", "emllink").contains(itemName)) {
                            try {
                                if (emlURI == null) {
                                    emlURI = URI.create(itemValue);
                                }
                            } catch (IllegalArgumentException ex) {
                                // ignore
                            }
                        } else if (Arrays.asList("ipt_dwca", "link").contains(itemName)) {
                            try {
                                if (archiveURI == null) {
                                    archiveURI = URI.create(itemValue);
                                }
                            } catch (IllegalArgumentException ex) {
                                // ignore
                            }

                        } else if (Arrays.asList("type", "archiveType").contains(itemName)) {
                            isDWCA = StringUtils.equals(itemValue, "DWCA");
                        }

                    }

                    IRI archiveParent = uuid == null ? parent1 : toUUID(uuid.toString());
                    if (uuid != null) {
                        emitter.emit(toStatement(parent1, HAD_MEMBER, archiveParent));
                    }

                    if (emlURI != null) {
                        IRI uriNode = toIRI(emlURI);
                        emitter.emit(toStatement(archiveParent, HAD_MEMBER, uriNode));
                        emitter.emit(toStatement(uriNode, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_EML)));
                        emitter.emit(toStatement(uriNode, HAS_VERSION, toBlank()));
                    }

                    if (isDWCA && archiveURI != null) {
                        IRI refNodeDWCAUri = toIRI(archiveURI.toString());
                        emitter.emit(toStatement(archiveParent, HAD_MEMBER, refNodeDWCAUri));

                        emitter.emit(toStatement(refNodeDWCAUri, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_DWCA)));
                        emitter.emit(toStatement(refNodeDWCAUri, HAS_VERSION, toBlank()));

                    }

                }
            }
        };

        XMLUtil.handleXPath("//item", handler, emitter, resourceAsStream);
    }

    static void parsePublishers(IRI parent, StatementEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                String publisherUUID = item.get("uuid").asText();
                IRI refNodePublisher = toUUID(publisherUUID);
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
            parsePublishers(refNode, this, get(refNode));
        } catch (IOException e) {
            LOG.warn("failed to parse [" + refNode.toString() + "]", e);
        }
    }

}
