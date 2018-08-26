package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.model.RefNodeURI;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.globalbioticinteractions.preston.model.RefNodeType.IDIGBIO_PUBLISHERS_JSON;

public class RegistryReaderIDigBio extends RefNodeProcessor {

    private final static Log LOG = LogFactory.getLog(RegistryReaderIDigBio.class);

    public RegistryReaderIDigBio(RefNodeListener listener) {
        super(listener);
    }

    @Override
    public void on(RefNode refNode) {
        if (refNode.equivalentTo(Seeds.SEED_NODE_IDIGBIO)) {
            String publishersURI = "https://search.idigbio.org/v2/search/publishers";
            RefNode publishers = new RefNodeString(refNode, RefNodeType.URI, publishersURI);
            RefNode publishersBlob = new RefNodeURI(publishers, IDIGBIO_PUBLISHERS_JSON, URI.create(publishersURI));
            emit(publishers);
            emit(publishersBlob);
        } else if (IDIGBIO_PUBLISHERS_JSON == refNode.getType()) {
            parsePublishers(refNode);
        } else if (RSS_TYPE_MAP.values().contains(refNode.getType())) {
            parse(refNode);
        }
    }

    public static final Map<String, RefNodeType> RSS_TYPE_MAP = new HashMap<String, RefNodeType>() {{
        put("feeder", RefNodeType.IDIGBIO_RSS_FEEDER);
        put("ipt", RefNodeType.IDIGBIO_RSS_IPT);
        put("rss", RefNodeType.IDIGBIO_RSS);
        put("symbiota", RefNodeType.IDIGBIO_RSS_SYMBIOTA);
    }};

    static void parseRssFeed(RefNode parent, RefNodeEmitter emitter, InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(resourceAsStream);
        XPathFactory xPathfactory = XPathFactory.newInstance();
        XPath xpath = xPathfactory.newXPath();
        XPathExpression expr = xpath.compile("//item");
        NodeList nl = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
        for (int i = 0; i < nl.getLength(); i++) {
            RefNodeType archiveType = null;
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
                } else if ("emllink".equals(itemName)) {
                    try {
                        emlURI = URI.create(itemValue);
                    } catch (IllegalArgumentException ex) {
                        // ignore
                    }
                } else if ("link".equals(itemName)) {
                    try {
                        archiveURI = URI.create(itemValue);
                    } catch (IllegalArgumentException ex) {
                        // ignore
                    }

                } else if (Arrays.asList("type", "archiveType").contains(itemName)) {
                    try {
                        if (archiveType == null && StringUtils.equals(itemValue, "DWCA")) {
                            archiveType = RefNodeType.DWCA;
                        }
                    } catch (IllegalArgumentException ex) {
                        // ignore
                    }

                }

            }

            RefNode archiveParent = uuid == null ? parent : new RefNodeString(parent, RefNodeType.UUID, uuid.toString());
            if (uuid != null) {
                emitter.emit(archiveParent);
            }

            if (emlURI != null) {
                RefNode uriNode = new RefNodeString(archiveParent, RefNodeType.URI, emlURI.toString());
                emitter.emit(uriNode);
                emitter.emit(new RefNodeURI(uriNode, RefNodeType.EML, emlURI));
            }

            if (RefNodeType.DWCA == archiveType && archiveURI != null) {
                RefNodeString refNodeDWCAUri = new RefNodeString(archiveParent, RefNodeType.URI, archiveURI.toString());
                emitter.emit(refNodeDWCAUri);
                emitter.emit(new RefNodeURI(refNodeDWCAUri, RefNodeType.DWCA, archiveURI));
            }
        }
    }

    static void parsePublishers(RefNode parent, RefNodeEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        if (r.has("items") && r.get("items").isArray()) {
            for (JsonNode item : r.get("items")) {
                String publisherUUID = item.get("uuid").asText();
                RefNodeString publisherUUIDNode = new RefNodeString(parent, RefNodeType.UUID, publisherUUID);
                emitter.emit(publisherUUIDNode);
                JsonNode data = item.get("data");
                if (item.has("data")) {
                    String rssFeedUrl = data.has("rss_url") ? data.get("rss_url").asText() : null;
                    if (StringUtils.isNotBlank(rssFeedUrl)) {
                        RefNodeString refNode = new RefNodeString(publisherUUIDNode, RefNodeType.URI, rssFeedUrl);
                        String publisherType = data.has("publisher_type") ? data.get("publisher_type").asText() : null;

                        RefNodeType type = RSS_TYPE_MAP.getOrDefault(publisherType, RefNodeType.IDIGBIO_RSS);
                        emitter.emit(refNode);
                        emitter.emit(new RefNodeURI(refNode, type, URI.create(rssFeedUrl)));
                    }
                }
            }
        }
    }

    private void parsePublishers(RefNode refNode) {
        try {
            parsePublishers(refNode, this, refNode.getData());
        } catch (IOException e) {
            LOG.warn("failed to parse [" + refNode.getLabel() + "] of type [" + refNode.getType() + "]");
        }
    }

    private void parse(RefNode refNode) {
        try {
            parseRssFeed(refNode, this, refNode.getData());
        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
            LOG.warn("failed to parse [" + refNode.getLabel() + "] of type [" + refNode.getType() + "]");
        }


    }
}
