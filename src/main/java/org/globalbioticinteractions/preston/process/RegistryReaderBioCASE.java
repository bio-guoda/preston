package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.MimeTypes;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.cmd.CrawlContext;
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
import java.util.stream.Stream;

import static org.globalbioticinteractions.preston.RefNodeConstants.*;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.*;

public class RegistryReaderBioCASE extends ProcessorReadOnly {
    private static final Log LOG = LogFactory.getLog(RegistryReaderBioCASE.class);

    static final String BIOCASE_REGISTRY_ENDPOINT = "https://bms.gfbio.org/services/data-sources/";
    public static final IRI BIO_CASE_REGISTRY = toIRI(BIOCASE_REGISTRY_ENDPOINT);

    // https://wiki.bgbm.org/bps/index.php/Archiving
    // http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=Herbar&inventory=1
    // https://bms.gfbio.org/services/data-sources/


    public RegistryReaderBioCASE(BlobStoreReadOnly blobStoreReadOnly, CrawlContext context, StatementListener listener) {
        super(blobStoreReadOnly, context, listener);
    }

    @Override
    public void on(Triple statement) {
        if (Seeds.BIOCASE.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream.of(
                    toStatement(RegistryReaderBioCASE.BIO_CASE_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a registry of RSS Feeds that point to publishers of Darwin Core archives, and EML descriptors.")),
                    toStatement(RegistryReaderBioCASE.BIO_CASE_REGISTRY, CREATED_BY, Seeds.BIOCASE),
                    toStatement(BIO_CASE_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(BIO_CASE_REGISTRY, HAS_VERSION, toBlank())).forEach(this::emit);

        } else if (hasVersionAvailable(statement)) {
            try {
                EmittingParser parse = null;
                if (BIO_CASE_REGISTRY.equals(getVersionSource(statement))) {
                    parse = RegistryReaderBioCASE::parseProviders;

                } else if (StringUtils.contains(getVersionSource(statement).toString(), "pywrapper.cgi?dsa=")) {
                    parse = RegistryReaderBioCASE::parseDatasetInventory;
                }
                if (parse != null) {
                    BlankNodeOrIRI version = getVersion(statement);
                    if (version instanceof IRI) {
                        InputStream content = get((IRI) version);
                        if (content != null) {
                            parse.parse(content, this);
                        }
                    }
                }


            } catch (IOException e) {
                LOG.warn("failed to read from " + getVersion(statement).toString(), e);
            }
        }

    }


    public static void parseProviders(InputStream providers, StatementEmitter emitter) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(providers);
        if (jsonNode.isArray()) {
            for (JsonNode provider : jsonNode) {
                if (provider.has("biocase_url") && provider.has("datasource")) {
                    String url = provider.get("biocase_url").asText();
                    String datasource = provider.get("datasource").asText();
                    if (StringUtils.isNotBlank(url) && StringUtils.isNotBlank(datasource)) {
                        URI uri = generateDataSourceAccessUrl(url, datasource);
                        if (uri != null) {
                            IRI refNode = toIRI(uri);
                            emitter.emit(toStatement(refNode, HAS_FORMAT, toLiteral(MimeTypes.XML)));
                            emitter.emit(toStatement(refNode, HAS_VERSION, toBlank()));
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

    public static void parseDatasetInventory(InputStream datasets, StatementEmitter emitter) throws IOException {
        XMLUtil.handleXPath("//dsi:archive", new XPathHandler() {
            @Override
            public void evaluateXPath(StatementEmitter emitter, NodeList nodeList) throws XPathExpressionException {

                Map<String, String> mapping = new HashMap<String, String>() {{
                    put("http://www.tdwg.org/schemas/abcd/2.06", MimeTypes.MIME_TYPE_ABCDA);
                }};

                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node item = nodeList.item(i);
                    NamedNodeMap attributes = item.getAttributes();
                    String url = item.getTextContent();

                    String type = resolveType(mapping, attributes);

                    if (StringUtils.isNotBlank(url) && StringUtils.isNotBlank(type)) {
                        IRI versionSource = toIRI(url);
                        emitter.emit(toStatement(versionSource, HAS_FORMAT, toLiteral(type)));
                        emitter.emit(toStatement(versionSource, HAS_VERSION, toBlank()));
                    }

                }

            }

            private String resolveType(Map<String, String> mapping, NamedNodeMap attributes) {
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


}
