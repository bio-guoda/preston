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
import org.globalbioticinteractions.preston.model.RefNodeFactory;
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

import static org.globalbioticinteractions.preston.RefNodeConstants.HAD_MEMBER;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_FORMAT;

public class RegistryReaderIDigBio extends ProcessorReadOnly {

    private final static Log LOG = LogFactory.getLog(RegistryReaderIDigBio.class);
    public static final String PUBLISHERS_URI = "https://search.idigbio.org/v2/search/publishers";
    public static final IRI PUBLISHERS = RefNodeFactory.toIRI(URI.create(PUBLISHERS_URI));

    public RegistryReaderIDigBio(BlobStoreReadOnly blobStore, RefStatementListener listener) {
        super(blobStore, listener);
    }

    @Override
    public void on(Triple statement) {
        if (statement.getSubject().equals(Seeds.SEED_NODE_IDIGBIO)) {
            IRI publishers = PUBLISHERS;
            emit(RefNodeFactory.toStatement(publishers, HAD_MEMBER, statement.getSubject()));
            emit(RefNodeFactory.toStatement(publishers, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_JSON)));
            emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.WAS_DERIVED_FROM, publishers));
        } else if (RefNodeFactory.hasDerivedContentAvailable(statement)) {
            parse(statement, (IRI) statement.getSubject());
        }
    }

    public void parse(Triple statement, IRI toBeParsed) {
        if (statement.getObject().equals(PUBLISHERS)) {
            if (!RefNodeFactory.isBlankOrSkolemizedBlank(statement.getSubject())) {
                parsePublishers(toBeParsed);
            }
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

    static void parseRssFeed(final IRI parent1, RefStatementEmitter emitter, InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {

        XPathHandler handler = new XPathHandler() {
            @Override
            public void evaluateXPath(RefStatementEmitter emitter, NodeList nodeList) throws XPathExpressionException {
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

                    IRI archiveParent = uuid == null ? parent1 : RefNodeFactory.toUUID(uuid.toString());
                    if (uuid != null) {
                        emitter.emit(RefNodeFactory.toStatement(parent1, HAD_MEMBER, archiveParent));
                    }

                    if (emlURI != null) {
                        IRI uriNode = RefNodeFactory.toIRI(emlURI);
                        emitter.emit(RefNodeFactory.toStatement(archiveParent, HAD_MEMBER, uriNode));
                        emitter.emit(RefNodeFactory.toStatement(uriNode, HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_EML)));
                        emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.WAS_DERIVED_FROM, uriNode));
                    }

                    if (isDWCA && archiveURI != null) {
                        IRI refNodeDWCAUri = RefNodeFactory.toIRI(archiveURI.toString());
                        emitter.emit(RefNodeFactory.toStatement(archiveParent, HAD_MEMBER, refNodeDWCAUri));

                        emitter.emit(RefNodeFactory.toStatement(refNodeDWCAUri, HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_DWCA)));
                        emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.WAS_DERIVED_FROM, refNodeDWCAUri));

                    }

                }
            }
        };

        XMLUtil.handleXPath("//item", handler, emitter, resourceAsStream);
    }

    static void parsePublishers(IRI parent, RefStatementEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                String publisherUUID = item.get("uuid").asText();
                IRI refNodePublisher = RefNodeFactory.toUUID(publisherUUID);
                emitter.emit(RefNodeFactory.toStatement(parent, RefNodeConstants.HAD_MEMBER, refNodePublisher));
                JsonNode data = item.get("data");
                if (item.has("data")) {
                    String rssFeedUrl = data.has("rss_url") ? data.get("rss_url").asText() : null;
                    if (StringUtils.isNotBlank(rssFeedUrl)) {
                        IRI refNodeFeed = RefNodeFactory.toIRI(rssFeedUrl);
                        emitter.emit(RefNodeFactory.toStatement(refNodePublisher, RefNodeConstants.HAD_MEMBER, refNodeFeed));
                        emitter.emit(RefNodeFactory.toStatement(refNodeFeed, HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_RSS)));
                        emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.WAS_DERIVED_FROM, refNodeFeed));
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
