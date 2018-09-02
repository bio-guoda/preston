package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.MimeTypes;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;
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
import static org.globalbioticinteractions.preston.RefNodeConstants.PUBLISHER_REGISTRY_OF;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_DERIVED_FROM;

public class RegistryReaderIDigBio extends ProcessorReadOnly {

    private final static Log LOG = LogFactory.getLog(RegistryReaderIDigBio.class);
    public static final String PUBLISHERS_URI = "https://search.idigbio.org/v2/search/publishers";
    public static final RefNode PUBLISHERS = RefNodeFactory.toURI(URI.create(PUBLISHERS_URI));

    public RegistryReaderIDigBio(BlobStoreReadOnly blobStore, RefStatementListener listener) {
        super(blobStore, listener);
    }

    @Override
    public void on(RefStatement statement) {
        if (statement.getSubject().equivalentTo(Seeds.SEED_NODE_IDIGBIO)) {
            RefNode publishers = PUBLISHERS;
            emit(RefNodeFactory.toStatement(publishers, PUBLISHER_REGISTRY_OF, statement.getSubject()));
            emit(RefNodeFactory.toStatement(publishers, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_JSON)));
            emit(RefNodeFactory.toStatement(null, WAS_DERIVED_FROM, publishers));
        } else if (statement.getObject().equivalentTo(PUBLISHERS)
                && RefNodeFactory.isDerivedFrom(statement)) {
            parsePublishers(statement.getSubject());
        } else if (RefNodeFactory.isDerivedFrom(statement)) {
            parse(statement.getSubject());
        }
    }


    private void parse(RefNode refNode) {
        try {
            parseRssFeed(refNode, this, get(refNode.getContentHash()));
        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
            // ignore - opportunistic parsing attempt
        }
    }

    static void parseRssFeed(final RefNode parent1, RefStatementEmitter emitter, InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {

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

                    RefNode archiveParent = uuid == null ? parent1 : RefNodeFactory.toUUID(uuid.toString());
                    if (uuid != null) {
                        emitter.emit(RefNodeFactory.toStatement(parent1, HAD_MEMBER, archiveParent));
                    }

                    if (emlURI != null) {
                        RefNode uriNode = RefNodeFactory.toURI(emlURI);
                        emitter.emit(RefNodeFactory.toStatement(archiveParent, HAD_MEMBER, uriNode));
                        emitter.emit(RefNodeFactory.toStatement(uriNode, HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_EML)));
                        emitter.emit(RefNodeFactory.toStatement(null, WAS_DERIVED_FROM, uriNode));
                    }

                    if (isDWCA && archiveURI != null) {
                        RefNode refNodeDWCAUri = RefNodeFactory.toURI(archiveURI.toString());
                        emitter.emit(RefNodeFactory.toStatement(archiveParent, HAD_MEMBER, refNodeDWCAUri));

                        emitter.emit(RefNodeFactory.toStatement(refNodeDWCAUri, HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_DWCA)));
                        emitter.emit(RefNodeFactory.toStatement(null, WAS_DERIVED_FROM, refNodeDWCAUri));

                    }

                }
            }
        };

        XMLUtil.handleXPath("//item", handler, emitter, resourceAsStream);
    }

    static void parsePublishers(RefNode parent, RefStatementEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                String publisherUUID = item.get("uuid").asText();
                RefNode refNodePublisher = RefNodeFactory.toUUID(publisherUUID);
                emitter.emit(RefNodeFactory.toStatement(parent, RefNodeConstants.HAD_MEMBER, refNodePublisher));
                JsonNode data = item.get("data");
                if (item.has("data")) {
                    String rssFeedUrl = data.has("rss_url") ? data.get("rss_url").asText() : null;
                    if (StringUtils.isNotBlank(rssFeedUrl)) {
                        RefNode refNodeFeed = RefNodeFactory.toURI(rssFeedUrl);
                        emitter.emit(RefNodeFactory.toStatement(refNodePublisher, RefNodeConstants.HAD_MEMBER, refNodeFeed));
                        emitter.emit(RefNodeFactory.toStatement(refNodeFeed, HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_RSS)));
                        emitter.emit(RefNodeFactory.toStatement(null, RefNodeConstants.WAS_DERIVED_FROM, refNodeFeed));
                    }
                }
            }
        }
    }

    private void parsePublishers(RefNode refNode) {
        try {
            parsePublishers(refNode, this, get(refNode.getContentHash()));
        } catch (IOException e) {
            LOG.warn("failed toLiteral parse [" + refNode.getLabel() + "]", e);
        }
    }

}
