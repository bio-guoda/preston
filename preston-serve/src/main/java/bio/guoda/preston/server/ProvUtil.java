package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ProvUtil {
    public static final Pattern URN_UUID_REQUEST_PATTERN
            = Pattern.compile("^urn:uuid:" + UUIDUtil.UUID_PATTERN_PART + "$");
    public static final String QUERY_TYPE_UUID = "uuid";
    public static final String QUERY_TYPE_CONTENT_ID = "contentId";
    public static final String QUERY_TYPE_URL = "url";
    public static final String QUERY_TYPE_DOI = "doi";
    public static final List<String> QUERIES_SUPPORTED = Arrays.asList(QUERY_TYPE_DOI, QUERY_TYPE_UUID, QUERY_TYPE_URL, QUERY_TYPE_CONTENT_ID);

    public static Map<String, String> findMostRecentContentId(IRI iri,
                                                              String paramName,
                                                              String sparqlEndpoint,
                                                              String contentType,
                                                              IRI provenanceAnchor) throws IOException, URISyntaxException {
        Map<String, String> provenanceInfo = getProvenanceInfo(iri, paramName, sparqlEndpoint, contentType, provenanceAnchor, false);
        if (provenanceInfo.size() == 0) {
            provenanceInfo = getProvenanceInfo(iri, paramName, sparqlEndpoint, contentType, provenanceAnchor, true);
        }
        return provenanceInfo;
    }

    private static Map<String, String> getProvenanceInfo(
            IRI iri,
            String paramName,
            String sparqlEndpoint,
            String contentType,
            IRI provenanceAnchor,
            boolean includeBlanks) throws IOException, URISyntaxException {
        String response = findProvenance(
                iri,
                paramName,
                sparqlEndpoint,
                contentType,
                provenanceAnchor,
                includeBlanks, 2
        );
        return extractProvenanceInfo(response);
    }

    protected static String findProvenance(IRI iri,
                                           String paramName,
                                           String sparqlEndpoint,
                                           String contentType,
                                           IRI provenanceAnchor,
                                           boolean includeBlanks,
                                           int maxResults) throws IOException, URISyntaxException {
        String queryString = generateQuery(
                iri, paramName, contentType, provenanceAnchor, includeBlanks, maxResults
        );

        URI query = new URI("https", "example.org", "/query", "query=" + queryString, null);

        URI endpoint = new URI(sparqlEndpoint + "?" + query.getRawQuery());

        IRI dataURI = RefNodeFactory.toIRI(endpoint);

        InputStream inputStream = ResourcesHTTP.asInputStream(dataURI);
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

    protected static String generateQuery(IRI iri,
                                          String paramName,
                                          String contentType,
                                          IRI provenanceAnchor,
                                          boolean includeBlanks,
                                          int maxResults) throws IOException {
        String queryTemplateName = paramName + ".rq";
        InputStream resourceAsStream = RedirectingServlet.class.getResourceAsStream(queryTemplateName);

        if (resourceAsStream == null) {
            throw new IOException("failed to location query template [" + queryTemplateName + "]");
        }
        String queryTemplate = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);

        String queryString = StringUtils
                .replace(queryTemplate, "?_" + paramName + "_iri", iri.toString())
                .replace("?_type", "\"" + contentType + "\"")
                .replace("?_limit", Integer.toString(maxResults))
                .replace("?_provenanceId_iri", provenanceAnchor.toString());

        if (includeBlanks) {
            queryString = StringUtils.replace(queryString, "FILTER", "# FILTER");
        }
        return queryString;
    }

    private static Map<String, String> extractProvenanceInfo(String response) throws JsonProcessingException {
        return extractProvenanceInfo(new ObjectMapper().readTree(response));
    }

    static Map<String, String> extractProvenanceInfo(JsonNode jsonNode) {
        Map<String, String> attributes = new TreeMap<String, String>();
        extractProvenanceInfo(jsonNode, new Consumer<JsonNode>() {

            @Override
            public void accept(JsonNode binding) {
                binding.fieldNames()
                        .forEachRemaining(key -> attributes.put(key, binding.get(key).asText()));

            }
        }, 1);
        return attributes;
    }

    static void extractProvenanceInfo(JsonNode jsonNode, Consumer<JsonNode> listener, int maxRecords) {
        AtomicInteger recordCount = new AtomicInteger(0);
        
        if (jsonNode.has("results")) {
            JsonNode result = jsonNode.get("results");
            if (result.has("bindings")) {
                for (JsonNode binding : result.get("bindings")) {
                    ObjectNode attributes = new ObjectMapper().createObjectNode();
                    binding.fieldNames()
                            .forEachRemaining(key -> attributes.put(key, binding.get(key).has("value") ? binding.get(key).get("value").asText() : ""));
                    if (recordCount.get() >= maxRecords) {
                        break;
                    }
                    listener.accept(attributes);
                    recordCount.incrementAndGet();
                }
            }
        }
    }

    static String queryTypeForRequestedId(String requestURI) {
        return Stream.of(requestURI)
                .map(req -> URN_UUID_REQUEST_PATTERN.matcher(req).matches() ? QUERY_TYPE_UUID : req)
                .map(req -> Pattern.compile("^(10[.])([^/]+)/(.*)$").matcher(req).matches() ? QUERY_TYPE_DOI : req)
                .map(req -> Pattern.compile("^http[s]{0,1}://[^ ]+").matcher(req).matches() ? QUERY_TYPE_URL : req)
                .map(req -> Pattern.compile("^hash://[a-zA-Z0-9]+/[a-f0-9]+$").matcher(req).matches() ? QUERY_TYPE_CONTENT_ID : req)
                .filter(QUERIES_SUPPORTED::contains)
                .findFirst()
                .orElse("unknown");
    }
}
