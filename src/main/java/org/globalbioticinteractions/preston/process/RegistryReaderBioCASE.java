package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.MimeTypes;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.store.Predicate;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class RegistryReaderBioCASE extends ProcessorReadOnly {
    private static final Log LOG = LogFactory.getLog(RegistryReaderBioCASE.class);

    static final String BIOCASE_REGISTRY_ENDPOINT = "https://bms.gfbio.org/services/data-sources/";
    private static final IRI REF_NODE_REGISTRY = RefNodeFactory.toIRI(BIOCASE_REGISTRY_ENDPOINT);

    // https://wiki.bgbm.org/bps/index.php/Archiving
    // http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=Herbar&inventory=1
    // https://bms.gfbio.org/services/data-sources/


    public RegistryReaderBioCASE(BlobStoreReadOnly blobStoreReadOnly, RefStatementListener listener) {
        super(blobStoreReadOnly, listener);
    }


    public static void parseProviders(InputStream providers, RefStatementEmitter emitter) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(providers);
        if (jsonNode.isArray()) {
            for (JsonNode provider : jsonNode) {
                if (provider.has("biocase_url") && provider.has("datasource")) {
                    String url = provider.get("biocase_url").asText();
                    String datasource = provider.get("datasource").asText();
                    if (StringUtils.isNotBlank(url) && StringUtils.isNotBlank(datasource)) {
                        URI uri = generateDataSourceAccessUrl(url, datasource);
                        if (uri != null) {
                            IRI refNode = RefNodeFactory.toIRI(uri);
                            emitter.emit(RefNodeFactory.toStatement(refNode, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral(MimeTypes.XML)));
                            emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), Predicate.WAS_DERIVED_FROM, refNode));
                        }
                    }
                }
            }
        }
    }

    static URI generateDataSourceAccessUrl(String url, String datasource) {
        String replace = StringUtils.replace(url, "\\/", "/");
        replace = replace.endsWith("/") ? replace : replace + "/";
        URI uri = null;
        try {
            uri = new URI(replace + "pywrapper.cgi?dsa=" + datasource + "&inventory=1");

        } catch (URISyntaxException e) {
            LOG.warn("found invalid url [" + replace + "]", e);
        }
        return uri;
    }

    public static void parseDatasetInventory(InputStream datasets, RefStatementEmitter emitter) throws IOException {
        XMLUtil.handleXPath("//dsi:archive", new XPathHandler() {
            @Override
            public void evaluateXPath(RefStatementEmitter emitter, NodeList nodeList) throws XPathExpressionException {

                Map<String, String> mapping = new HashMap<String, String>() {{
                    put("http://www.tdwg.org/schemas/abcd/2.06", MimeTypes.MIME_TYPE_ABCDA);
                }};

                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node item = nodeList.item(i);
                    NamedNodeMap attributes = item.getAttributes();
                    String url = item.getTextContent();

                    String type = resolveType(mapping, item, attributes);

                    if (StringUtils.isNotBlank(url) && StringUtils.isNotBlank(type)) {
                        IRI object = RefNodeFactory.toIRI(url);
                        emitter.emit(RefNodeFactory.toStatement(object, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral(type)));
                        emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), Predicate.WAS_DERIVED_FROM, object));
                    }

                }

            }

            private String resolveType(Map<String, String> mapping, Node item, NamedNodeMap attributes) {
                String type = null;
                Node namespaceNode = attributes.getNamedItem("namespace");
                if (namespaceNode != null) {
                    String namespace = namespaceNode.getTextContent();
                    if (mapping.containsKey(namespace)) {
                        type = mapping.get(namespace);
                    }
                } else {
                    type = attributes.getNamedItem("rowType") == null ? null : MimeTypes.MIME_TYPE_DWCA;
                }
                return type;
            }
        }, emitter, datasets);
    }


    @Override
    public void on(Triple statement) {
        if (Seeds.SEED_NODE_BIOCASE.equals(statement.getSubject())
                && RefNodeConstants.HAD_MEMBER.equals(statement.getPredicate())) {
            emit(RefNodeFactory.toStatement(REF_NODE_REGISTRY, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_JSON)));
            emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), Predicate.WAS_DERIVED_FROM, REF_NODE_REGISTRY));
        } else if (RefNodeFactory.hasDerivedContentAvailable(statement)) {
            try {
                EmittingParser parse = null;
                if (REF_NODE_REGISTRY.equals(statement.getObject())) {
                    parse = RegistryReaderBioCASE::parseProviders;

                } else if (StringUtils.contains(statement.getObject().toString(), "pywrapper.cgi?dsa=")) {
                    parse = RegistryReaderBioCASE::parseDatasetInventory;
                }
                if (parse != null) {
                    if (statement.getSubject() instanceof IRI) {
                        InputStream content = get((IRI) statement.getSubject());
                        if (content != null) {
                            parse.parse(content, this);
                        }
                    }
                }


            } catch (IOException e) {
                LOG.warn("failed to read from [" + statement.getSubject().toString() + "]", e);
            }
        }

    }

}
