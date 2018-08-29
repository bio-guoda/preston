package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;

import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_CONTENT;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_PART;
import static org.globalbioticinteractions.preston.RefNodeConstants.PUBLISHER_REGISTRY_OF;

public class RegistryReaderIDigBio extends RefNodeProcessor {

    private final static Log LOG = LogFactory.getLog(RegistryReaderIDigBio.class);
    public static final String PUBLISHERS_URI = "https://search.idigbio.org/v2/search/publishers";
    public static final RefNodeString REF_NODE_STRING = new RefNodeString(PUBLISHERS_URI);

    public RegistryReaderIDigBio(RefNodeListener listener) {
        super(listener);
    }

    @Override
    public void on(RefNodeRelation relation) {
        if (relation.getTarget().equivalentTo(Seeds.SEED_NODE_IDIGBIO)) {
            RefNode publishers = REF_NODE_STRING;
            emit(new RefNodeRelation(relation.getTarget(), PUBLISHER_REGISTRY_OF, publishers));

            emit(new RefNodeRelation(publishers, HAS_CONTENT, null));
        } else if (relation.getTarget() != null
                && relation.getSource().equivalentTo(REF_NODE_STRING)
                && relation.getRelationType().equivalentTo(RefNodeConstants.HAS_CONTENT)) {
            parsePublishers(relation.getTarget());
        } else if (relation.getRelationType().equivalentTo(RefNodeConstants.HAS_CONTENT)) {
            parse(relation.getTarget());
        }
    }


    private void parse(RefNode refNode) {
        try {
            parseRssFeed(refNode, this, refNode.getData());
        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
            LOG.warn("failed to parse [" + refNode.getLabel() + "]", e);
        }
    }

    static void parseRssFeed(RefNode parent, RefNodeEmitter emitter, InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(resourceAsStream);
        XPathFactory xPathfactory = XPathFactory.newInstance();
        XPath xpath = xPathfactory.newXPath();
        XPathExpression expr = xpath.compile("//item");
        NodeList nl = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        for (int i = 0; i < nl.getLength(); i++) {
            boolean isDWCA = false;
            URI archiveURI = null;
            URI emlURI = null;
            UUID uuid = null;
            Node item = nl.item(i);
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

            RefNode archiveParent = uuid == null ? parent : new RefNodeString(uuid.toString());
            if (uuid != null) {
                emitter.emit(new RefNodeRelation(parent, HAS_PART, archiveParent));
            }

            if (emlURI != null) {
                RefNode uriNode = new RefNodeString(emlURI.toString());
                emitter.emit(new RefNodeRelation(archiveParent, HAS_PART, uriNode));
                emitter.emit(new RefNodeRelation(uriNode, HAS_CONTENT, null));
            }

            if (isDWCA && archiveURI != null) {
                RefNodeString refNodeDWCAUri = new RefNodeString(archiveURI.toString());
                emitter.emit(new RefNodeRelation(archiveParent, HAS_PART, refNodeDWCAUri));
                emitter.emit(new RefNodeRelation(refNodeDWCAUri, HAS_CONTENT, null));

            }

        }
    }

    static void parsePublishers(RefNode parent, RefNodeEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                String publisherUUID = item.get("uuid").asText();
                RefNodeString refNodePublisher = new RefNodeString(publisherUUID);
                emitter.emit(new RefNodeRelation(parent, RefNodeConstants.HAS_PART, refNodePublisher));
                JsonNode data = item.get("data");
                if (item.has("data")) {
                    String rssFeedUrl = data.has("rss_url") ? data.get("rss_url").asText() : null;
                    if (StringUtils.isNotBlank(rssFeedUrl)) {
                        RefNodeString refNodeFeed = new RefNodeString(rssFeedUrl);
                        emitter.emit(new RefNodeRelation(refNodePublisher, RefNodeConstants.HAS_FEED, refNodeFeed));
                        emitter.emit(new RefNodeRelation(refNodeFeed, RefNodeConstants.HAS_CONTENT, null));
                    }
                }
            }
        }
    }

    private void parsePublishers(RefNode refNode) {
        try {
            parsePublishers(refNode, this, refNode.getData());
        } catch (IOException e) {
            LOG.warn("failed to parse [" + refNode.getLabel() + "]", e);
        }
    }

}
