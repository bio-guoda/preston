package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ProvUtil {
    public static final Pattern URN_UUID_REQUEST_PATTERN
            = Pattern.compile("^urn:uuid:" + UUIDUtil.UUID_PATTERN_PART + "$");
    public static final String QUERY_TYPE_UUID = "uuid";
    public static final String QUERY_TYPE_URL = "url";
    public static final String QUERY_TYPE_DOI = "doi";

    public static Map<String, String> findMostRecentContentId(IRI iri, String paramName, String sparqlEndpoint) throws IOException, URISyntaxException {
        String response = findProvenance(iri, paramName, sparqlEndpoint);
        return extractProvenanceInfo(response);
    }

    protected static String findProvenance(IRI iri, String paramName, String sparqlEndpoint) throws IOException, URISyntaxException {
        InputStream resourceAsStream = RedirectingServlet.class.getResourceAsStream(paramName + ".rq");

        String queryTemplate = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);

        String queryString = StringUtils.replace(queryTemplate, "?_" + paramName + "_iri", iri.toString());

        URI query = new URI("https", "example.org", "/query", "query=" + queryString, null);

        URI endpoint = new URI(sparqlEndpoint + "?" + query.getRawQuery());

        IRI dataURI = RefNodeFactory.toIRI(endpoint);

        InputStream inputStream = ResourcesHTTP.asInputStream(dataURI);
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

    private static Map<String, String> extractProvenanceInfo(String response) throws JsonProcessingException {
        return extractProvenanceInfo(new ObjectMapper().readTree(response));
    }

    static Map<String, String> extractProvenanceInfo(JsonNode jsonNode) {
        Map<String, String> attributes = new TreeMap<String, String>();
        if (jsonNode.has("results")) {
            JsonNode result = jsonNode.get("results");
            if (result.has("bindings")) {
                for (JsonNode binding : result.get("bindings")) {
                    binding.fieldNames()
                            .forEachRemaining(key -> attributes.put(key, binding.get(key).has("value") ? binding.get(key).get("value").asText() : ""));
                    break;
                }
            }

        }
        return attributes;
    }

    static String queryTypeForRequestedId(String requestURI) {
        return Stream.of(requestURI)
                .map(req -> URN_UUID_REQUEST_PATTERN.matcher(req).matches() ? QUERY_TYPE_UUID : req)
                .map(req -> Pattern.compile("^(10[.])([^/]+)/(.*)$").matcher(req).matches() ? QUERY_TYPE_DOI : req)
                .map(req -> Pattern.compile("^http[s]{0,1}://[^ ]+").matcher(req).matches() ? QUERY_TYPE_URL : req)
                .filter(type -> Arrays.asList(QUERY_TYPE_DOI, QUERY_TYPE_UUID, QUERY_TYPE_URL).contains(type))
                .findFirst()
                .orElse("unknown");
    }
}
