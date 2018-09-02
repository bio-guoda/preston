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
import org.globalbioticinteractions.preston.model.RefStatement;
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

import static org.globalbioticinteractions.preston.RefNodeConstants.SEED_OF;

public class RegistryReaderBioCASE extends ProcessorReadOnly {
    private static final Log LOG = LogFactory.getLog(RegistryReaderBioCASE.class);

    static final String BIOCASE_REGISTRY_ENDPOINT = "https://bms.gfbio.org/services/data-sources/";
    private static final RefNode REF_NODE_REGISTRY = RefNodeFactory.toURI(BIOCASE_REGISTRY_ENDPOINT);

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
                            RefNode refNode = RefNodeFactory.toURI(uri);
                            emitter.emit(new RefStatement(refNode, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral(MimeTypes.XML)));
                            emitter.emit(new RefStatement(null, RefNodeConstants.WAS_DERIVED_FROM, refNode));
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
                        RefNode object = RefNodeFactory.toURI(url);
                        emitter.emit(new RefStatement(object, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral(type)));
                        emitter.emit(new RefStatement(null, RefNodeConstants.WAS_DERIVED_FROM, object));
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
    public void on(RefStatement statement) {
        if (Seeds.SEED_NODE_BIOCASE.equivalentTo(statement.getSubject())
                && SEED_OF.equivalentTo(statement.getPredicate())) {
            emit(new RefStatement(REF_NODE_REGISTRY, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_JSON)));
            emit(new RefStatement(null, RefNodeConstants.WAS_DERIVED_FROM, REF_NODE_REGISTRY));
        } else if (RefNodeFactory.isDerivedFrom(statement)) {
            try {
                EmittingParser parse = null;
                if (REF_NODE_REGISTRY.equivalentTo(statement.getObject())) {
                    parse = RegistryReaderBioCASE::parseProviders;

                } else if (StringUtils.contains(statement.getObject().getLabel(), "pywrapper.cgi?dsa=")) {
                    parse = RegistryReaderBioCASE::parseDatasetInventory;
                }
                if (parse != null) {
                    InputStream content = get(statement.getSubject().getContentHash());
                    if (content != null) {
                        parse.parse(content, this);
                    }
                }


            } catch (IOException e) {
                LOG.warn("failed toLiteral read from [" + statement.getSubject().getLabel() + "]", e);
            }
        }

    }

}
