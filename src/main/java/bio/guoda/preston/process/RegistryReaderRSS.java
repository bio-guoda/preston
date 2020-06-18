package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class RegistryReaderRSS extends ProcessorReadOnly {
    public RegistryReaderRSS(BlobStoreReadOnly blobStore, StatementsListener listener) {
        super(blobStore, listener);

    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            List<Quad> nodes = new ArrayList<>();
            parse((IRI) getVersion(statement), new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, this);
            if (!nodes.isEmpty()) { // Since this is opportunistic, only record an activity if something was produced
                ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
            }
        }

    }

    protected static void parse(IRI iri, StatementsEmitter emitter, KeyValueStoreReadOnly readOnlyStore) {
        try {
            // first parsePublishers document to check whether it is valid
            checkParsebleXml(iri, readOnlyStore);
            // then parsePublishers
            parseRssFeed(iri, emitter, readOnlyStore);
        } catch (IOException e) {
            // ignore - opportunistic parsing attempt
        }
    }

    private static void parseRssFeed(IRI iri, StatementsEmitter emitter, KeyValueStoreReadOnly readOnlyStore) throws IOException {
        try (InputStream in2 = readOnlyStore.get(iri)) {
            if (in2 != null) {
                /// then parsePublishers
                parseRssFeed(iri, emitter, in2);
            }
        }
    }

    private static void checkParsebleXml(IRI iri, KeyValueStoreReadOnly readOnlyStore) throws IOException {
        try (InputStream in = readOnlyStore.get(iri)) {
            if (in != null) {
                new XmlMapper().readTree(in);
            }
        }
    }

    private static void parseRssFeed(final IRI parent1, StatementsEmitter emitter, InputStream resourceAsStream) throws IOException {

        XPathHandler handler = new XPathHandler() {
            @Override
            public void evaluateXPath(StatementsEmitter emitter, NodeList nodeList) {
                for (int i = 0; i < nodeList.getLength(); i++) {
                    boolean isDWCA = false;
                    URI linkURL = null;
                    URI dwcaURL = null;
                    URI emlURI = null;
                    String uuid = null;
                    Node item = nodeList.item(i);
                    NodeList childNodes = item.getChildNodes();
                    for (int j = 0; j < childNodes.getLength(); j++) {
                        Node child = childNodes.item(j);
                        String itemName = child.getNodeName();
                        String itemValue = child.getTextContent();

                        if ("guid".equals(itemName)) {
                            uuid = StringUtils.trim(itemValue);
                        } else if (Arrays.asList("ipt_eml", "emllink", "ipt:eml").contains(itemName)) {
                            emlURI = generateURI(emlURI, itemValue);
                        } else if (Arrays.asList("ipt_dwca", "ipt:dwca").contains(itemName)) {
                            dwcaURL = generateURI(dwcaURL, itemValue);
                        } else if ("link".equals(itemName)) {
                            linkURL = generateURI(linkURL, itemValue);
                        } else if (Arrays.asList("type", "archiveType").contains(itemName)) {
                            isDWCA = StringUtils.equals(StringUtils.lowerCase(StringUtils.trim(itemValue)), "dwca");
                        }

                    }

                    IRI archiveParent = uuid == null ? parent1 : toIRI(uuid);
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
                    return URI.create(StringUtils.trim(itemValue));
                } catch (IllegalArgumentException ex) {
                    // ignore
                }
                return uri;
            }
        };

        XMLUtil.handleXPath("//item", handler, emitter, resourceAsStream);
    }

}
