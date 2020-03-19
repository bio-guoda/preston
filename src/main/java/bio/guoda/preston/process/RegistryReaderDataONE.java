package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.Quad;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.model.RefNodeFactory.fromUUID;

public class RegistryReaderDataONE extends ProcessorReadOnly {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("eml://ecoinformatics.org/eml-2.1.1", MimeTypes.MIME_TYPE_EML);
    }};


    public static final String DATA_ONE_URL_BASE = "//cn.dataone.org/cn/v2";
    public static final String DATA_ONE_URL_RESOLVE_PREFIX = DATA_ONE_URL_BASE + "/resolve";
    public static final String DATA_ONE_URL_QUERY_PREFIX = DATA_ONE_URL_BASE + "/query";
    public static final String DATA_ONE_URL_QUERY_PART = DATA_ONE_URL_QUERY_PREFIX + "/solr/?q=formatId:eml*+AND+-obsoletedBy:*&fl=identifier,dataUrl&wt=json&start=0&rows=100";
    public static final String DATA_ONE_REGISTRY_STRING = "http:" + DATA_ONE_URL_QUERY_PART;
    private final Log LOG = LogFactory.getLog(RegistryReaderDataONE.class);
    public static final IRI DATA_ONE_REGISTRY = toIRI(DATA_ONE_REGISTRY_STRING);

    public RegistryReaderDataONE(BlobStoreReadOnly blobStoreReadOnly, StatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.DATA_ONE.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            ArrayList<Quad> nodes = new ArrayList<Quad>();
            Stream.of(
                    toStatement(Seeds.DATA_ONE, IS_A, ORGANIZATION),
                    toStatement(RegistryReaderDataONE.DATA_ONE_REGISTRY, DESCRIPTION, toEnglishLiteral("Data Observation Network for Earth (DataONE) is the foundation of new innovative environmental science through a distributed framework and sustainable cyberinfrastructure that meets the needs of science and society for open, persistent, robust, and secure access to well-described and easily discovered Earth observational data. DataONE provides a registry EML descriptors.")),
                    toStatement(RegistryReaderDataONE.DATA_ONE_REGISTRY, CREATED_BY, Seeds.DATA_ONE))
                    .forEach(nodes::add);
            emitPageRequest(nodes::add, DATA_ONE_REGISTRY);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(DATA_ONE_URL_QUERY_PREFIX)) {
            ArrayList<Quad> nodes = new ArrayList<Quad>();
            try {
                IRI currentPage = (IRI) getVersion(statement);
                InputStream in = get(currentPage);
                if (in != null) {
                    parse(currentPage, nodes::add, in, getVersionSource(statement));
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(DATA_ONE_URL_RESOLVE_PREFIX)) {
            ArrayList<Quad> nodes = new ArrayList<Quad>();
            try {
                IRI currentPage = (IRI) getVersion(statement);
                parseObjectLocationList(nodes::add, get(currentPage));
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    private void parseObjectLocationList(StatementEmitter emitter, InputStream inputStream) throws IOException {

        XMLUtil.handleXPath("//identifier", new XPathHandler() {
            @Override
            public void evaluateXPath(StatementEmitter emitter, NodeList nodeList) throws XPathExpressionException {

                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node item = nodeList.item(i);
                    String identifier = item.getTextContent();
                    Node parent = item.getParentNode();
                    NodeList childNodes = parent.getChildNodes();
                    for (int j = 0; j < childNodes.getLength(); j++) {
                        Node child = childNodes.item(j);
                        if ("objectLocation".equals(child.getLocalName())) {
                            for (int k = 0; k < child.getChildNodes().getLength(); k++) {
                                Node objectLocationProperty = child.getChildNodes().item(k);
                                if ("url".equals(objectLocationProperty.getLocalName())) {
                                    String urlString = objectLocationProperty.getTextContent();
                                    if (StringUtils.isNotBlank(identifier) && StringUtils.isNotBlank(urlString)) {
                                        IRI emlIRI = toIRI(urlString);
                                        emitter.emit(toStatement(toIRI(identifier), HAD_MEMBER, emlIRI));
                                        emitter.emit(toStatement(emlIRI, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_EML)));
                                        emitter.emit(toStatement(emlIRI, HAS_VERSION, toBlank()));

                                    }
                                } else if ("nodeIdentifier".equals(objectLocationProperty.getLocalName())) {
                                    String nodeIdentifier = objectLocationProperty.getTextContent();
                                    if (StringUtils.isNotBlank(nodeIdentifier) && StringUtils.isNotBlank(identifier)) {
                                        emitter.emit(toStatement(toIRI(nodeIdentifier), HAD_MEMBER, toIRI(identifier)));
                                    }
                                }
                            }
                        }
                    }

                }

            }
        }, emitter, inputStream);
    }

    static void emitNextPage(int offset, int limit, StatementEmitter emitter, String versionSourceURI) {
        String nextPageURL = versionSourceURI;
        nextPageURL = StringUtils.replacePattern(nextPageURL, "rows=[0-9]*", "rows=" + limit);
        nextPageURL = StringUtils.replacePattern(nextPageURL, "start=[0-9]*", "start=" + offset);
        nextPageURL = StringUtils.contains(nextPageURL, "?") ? nextPageURL : nextPageURL + "?";
        nextPageURL = StringUtils.contains(nextPageURL, "start") ? nextPageURL : nextPageURL + "&start=" + offset;
        nextPageURL = StringUtils.contains(nextPageURL, "rows") ? nextPageURL : nextPageURL + "&rows=" + limit;
        nextPageURL = StringUtils.replace(nextPageURL, "?&", "?");
        IRI nextPage = toIRI(nextPageURL);
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(StatementEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, CREATED_BY, Seeds.DATA_ONE),
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parse(IRI currentPage, StatementEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("response")) {
                JsonNode result = jsonNode.get("response");
                if (result.has("docs")) {
                    for (JsonNode doc : result.get("docs")) {
                        parseIndividualDataset(currentPage, emitter, doc);
                    }
                }

                if (jsonNode.has("responseHeader")) {
                    JsonNode header = jsonNode.get("responseHeader");
                    if (header.has("params")) {
                        JsonNode params = header.get("params");
                        if (params.has("start") && params.has("rows")) {
                            int offset = params.get("start").asInt();
                            int limit = params.get("rows").asInt();
                            int numFound = result.has("numFound") ? result.get("numFound").asInt() : 0;
                            String previousURL = versionSource.getIRIString();
                            int offsetNext = offset + limit;
                            if (offsetNext < numFound) {
                                emitNextPage(offsetNext, limit, emitter, previousURL);
                            }
                        }
                    }

                }
            }
        }


    }

    public static void parseIndividualDataset(IRI currentPage, StatementEmitter emitter, JsonNode result) {
        if (result.has("identifier")) {
            String uuid = result.get("identifier").asText();
            IRI datasetUUID = fromUUID(uuid);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetUUID));

            if (result.has("dataUrl")) {
                handleEndpoints(emitter, result, datasetUUID);
            }
        }
    }

    public static void handleEndpoints(StatementEmitter emitter, JsonNode doc, IRI datasetUUID) {
        if (doc.has("dataUrl")) {
            String urlString = doc.get("dataUrl").asText();
            IRI dataArchive = toIRI(urlString);
            emitter.emit(toStatement(datasetUUID, HAD_MEMBER, dataArchive));
            emitter.emit(toStatement(dataArchive, HAS_FORMAT, toContentType(MimeTypes.XML)));
            emitter.emit(toStatement(dataArchive, HAS_VERSION, toBlank()));
        }
    }

}
