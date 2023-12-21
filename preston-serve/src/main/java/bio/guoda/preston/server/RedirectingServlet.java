package bio.guoda.preston.server;

import bio.guoda.preston.HashType;
import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpHeaders;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.server.PropertyNames.PRESTON_CONTENT_RESOLVER_ENDPONT;
import static bio.guoda.preston.server.PropertyNames.PRESTON_SPARQL_ENDPONT;

public class RedirectingServlet extends HttpServlet {

    public static final Pattern URN_UUID_REQUEST_PATTERN
            = Pattern.compile("^urn:uuid:" + UUIDUtil.UUID_PATTERN_PART + "$");
    public static final String SEEN_AT = "seenAt";
    public static final String PROVENANCE_ID = "provenanceId";
    public static final String ARCHIVE_URL = "archiveUrl";
    public static final String UUID = "uuid";
    public static final String DOI = "doi";
    public static final String CONTENT_ID = "contentId";
    public static final String ACTIVITY = "activity";
    public static final String QUERY_TYPE_UUID = "uuid";
    public static final String QUERY_TYPE_HASH = "hash";
    public static final String QUERY_TYPE_URL = "url";
    public static final String QUERY_TYPE_DOI = "doi";

    @Override
    public void destroy() {
        log("destroying [" + this.getServletName() + "]");
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        String resolverEndpoint = getInitParameter(PRESTON_CONTENT_RESOLVER_ENDPONT);
        String sparqlEndpoint = getInitParameter(PRESTON_SPARQL_ENDPONT);

        String requestedId = parseRequestedIdOrThrow(request.getRequestURI());

        String queryType = queryTypeForRequestedId(requestedId);

        if (StringUtils.equals(queryType, QUERY_TYPE_DOI)) {
            requestedId = "https://doi.org/" + requestedId;
        }

        IRI requestedIdIRI = RefNodeFactory.toIRI(requestedId);

        if (StringUtils.equals("unknown", queryType)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            log("attempting to direct [" + requestedId + "] as [" + queryType + "]");
            handleRequest(response, resolverEndpoint, sparqlEndpoint, queryType, requestedIdIRI);

        }
    }

    protected void handleRequest(HttpServletResponse response, String resolverEndpoint, String sparqlEndpoint, String queryType, IRI requestedIdIRI) throws IOException, ServletException {
        try {
            Map<String, String> provInfo = findMostRecentContentId(
                    requestedIdIRI,
                    queryType,
                    sparqlEndpoint);
            if (provInfo.containsKey(CONTENT_ID) && StringUtils.isNotBlank(provInfo.get(CONTENT_ID))) {
                String contentId = provInfo.get(CONTENT_ID);
                URI uri = HashKeyUtil.insertSlashIfNeeded(URI.create(resolverEndpoint), contentId);
                response.setHeader(HttpHeaders.LOCATION, uri.toString());
                response.setHeader(HttpHeaders.CONTENT_TYPE, MimeTypes.MIME_TYPE_DWCA);
                response.setHeader(HttpHeaders.ETAG, contentId);
                response.setHeader(HttpHeaders.CONTENT_LOCATION, provInfo.get(ARCHIVE_URL));
                response.setHeader(HttpHeaders.DATE, provInfo.get(SEEN_AT));
                response.setHeader("X-UUID", provInfo.get(UUID));
                response.setHeader("X-DOI", provInfo.get(DOI));
                response.setHeader("X-PROVENANCE-ANCHOR", provInfo.get(PROVENANCE_ID));
                response.setHeader("X-PROVENANCE-ACTIVITY", provInfo.get(ACTIVITY));
                response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                log("response [" + requestedIdIRI.getIRIString() + "]");
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (Throwable th) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            log("not found [" + requestedIdIRI.getIRIString() + "]");
            try {
                throw th;
            } catch (URISyntaxException e) {
                throw new ServletException(e);
            }
        }
    }

    private String parseRequestedIdOrThrow(String requestURI) throws ServletException {
        log("request [" + requestURI + "]");


        return Stream.of(requestURI).filter(req -> StringUtils.startsWith(req, "/"))
                .map(req -> StringUtils.substring(req, 1))
                .findFirst()
                .orElseThrow(() -> new ServletException("invalid request [" + requestURI + "]"));
    }

    static String queryTypeForRequestedId(String requestURI) {
        return Stream.of(requestURI)
                .map(req -> URN_UUID_REQUEST_PATTERN.matcher(req).matches() ? QUERY_TYPE_UUID : req)
                .map(req -> HashType.sha1.getIRIPattern().matcher(req).matches() ? QUERY_TYPE_HASH : req)
                .map(req -> HashType.md5.getIRIPattern().matcher(req).matches() ? QUERY_TYPE_HASH : req)
                .map(req -> HashType.sha256.getIRIPattern().matcher(req).matches() ? QUERY_TYPE_HASH : req)
                .map(req -> Pattern.compile("^(10[.])([^/]+)/(.*)$").matcher(req).matches() ? QUERY_TYPE_DOI : req)
                .map(req -> Pattern.compile("^http[s]{0,1}://[^ ]+").matcher(req).matches() ? QUERY_TYPE_URL : req)
                .findFirst()
                .orElse("unknown");
    }

    static Map<String, String> findMostRecentContentId(IRI iri, String paramName, String sparqlEndpoint) throws IOException, URISyntaxException {
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


}