package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
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

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.model.RefNodeFactory.fromUUID;

public class RegistryReaderRSS extends ProcessorReadOnly {
    public RegistryReaderRSS(BlobStoreReadOnly testBlobStore, StatementListener listener) {
        super(testBlobStore, listener);

    }

    @Override
    public void on(Triple statement) {
        if (hasVersionAvailable(statement)) {
            parse((IRI) getVersion(statement));
        }

    }

    private void parse(IRI iri) {
        try {
            // first parse document to check whether it is valid
            InputStream in = get(iri);
            if (in != null) {
                new XmlMapper().readTree(in);
                /// then parse
                parseRssFeed(iri, this, in);
            }
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
                    URI linkURL = null;
                    URI dwcaURL = null;
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
                        } else if (Arrays.asList("ipt_eml", "emllink", "ipt:eml").contains(itemName)) {
                            emlURI = generateURI(emlURI, itemValue);
                        } else if (Arrays.asList("ipt_dwca", "ipt:dwca").contains(itemName)) {
                            dwcaURL = generateURI(dwcaURL, itemValue);
                        } else if ("link".equals(itemName)) {
                            linkURL = generateURI(linkURL, itemValue);
                        } else if (Arrays.asList("type", "archiveType").contains(itemName)) {
                            isDWCA = StringUtils.equals(itemValue, "DWCA");
                        }

                    }

                    IRI archiveParent = uuid == null ? parent1 : fromUUID(uuid.toString());
                    if (uuid != null) {
                        emitter.emit(toStatement(parent1, HAD_MEMBER, archiveParent));
                    }

                    if (emlURI != null) {
                        IRI uriNode = toIRI(emlURI);
                        emitter.emit(toStatement(archiveParent, HAD_MEMBER, uriNode));
                        emitter.emit(toStatement(uriNode, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_EML)));
                        emitter.emit(toStatement(uriNode, HAS_VERSION, toBlank()));
                    }

                    if (linkURL != null && isDWCA && dwcaURL == null) {
                        dwcaURL = linkURL;
                    }

                    if (dwcaURL != null) {
                        IRI refNodeDWCAUri = toIRI(dwcaURL.toString());
                        emitter.emit(toStatement(archiveParent, HAD_MEMBER, refNodeDWCAUri));

                        emitter.emit(toStatement(refNodeDWCAUri, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_DWCA)));
                        emitter.emit(toStatement(refNodeDWCAUri, HAS_VERSION, toBlank()));

                    }

                }
            }

            public URI generateURI(URI uri, String itemValue) {
                try {
                    return URI.create(itemValue);
                } catch (IllegalArgumentException ex) {
                    // ignore
                }
                return uri;
            }
        };

        XMLUtil.handleXPath("//item", handler, emitter, resourceAsStream);
    }

}
