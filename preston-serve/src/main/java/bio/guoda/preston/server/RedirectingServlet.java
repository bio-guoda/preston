package bio.guoda.preston.server;

import bio.guoda.preston.HashType;
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
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.server.PropertyNames.PRESTON_CONTENT_RESOLVER_ENDPONT;
import static bio.guoda.preston.server.PropertyNames.PRESTON_SPARQL_ENDPONT;

public class RedirectingServlet extends HttpServlet {

    public static final Pattern URN_UUID_REQUEST_PATTERN
            = Pattern.compile("^urn:uuid:" + UUIDUtil.UUID_PATTERN_PART + "$");

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

        if (StringUtils.equals(queryType, "doi")) {
            requestedId = "https://doi.org/" + requestedId;
        }

        IRI requestedIdIRI = RefNodeFactory.toIRI(requestedId);

        if (StringUtils.equals("unknown", queryType)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            log("attempting to direct [" + requestedId + "] as [" + queryType + "]");
            try {
                String contentId = findMostRecentContentId(
                        requestedIdIRI,
                        queryType,
                        sparqlEndpoint);
                if (StringUtils.isNotBlank(contentId)) {
                    URI uri = HashKeyUtil.insertSlashIfNeeded(URI.create(resolverEndpoint),  contentId);
                    response.setHeader(HttpHeaders.LOCATION, uri.toString());
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
                .map(req -> URN_UUID_REQUEST_PATTERN.matcher(req).matches() ? "uuid" : req)
                .map(req -> HashType.sha1.getIRIPattern().matcher(req).matches() ? "hash" : req)
                .map(req -> HashType.md5.getIRIPattern().matcher(req).matches() ? "hash" : req)
                .map(req -> HashType.sha256.getIRIPattern().matcher(req).matches() ? "hash" : req)
                .map(req -> Pattern.compile("^(10[.])([^/]+)/(.*)$").matcher(req).matches() ? "doi" : req)
                .map(req -> Pattern.compile("^http[s]{0,1}://[^ ]+").matcher(req).matches() ? "url" : req)
                .findFirst()
                .orElse("unknown");
    }

    public static String findMostRecentContentId(IRI iri1, String paramName, String sparqlEndpoint) throws IOException, URISyntaxException {
        String iri = iri1.toString();

        InputStream resourceAsStream = RedirectingServlet.class.getResourceAsStream(paramName + ".rq");

        String queryTemplate = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);

        String queryString = StringUtils.replace(queryTemplate, "?_" + paramName + "_iri", iri);

        URI query = new URI("https", "example.org", "/query", "query=" + queryString, null);

        URI endpoint = new URI(sparqlEndpoint + "?" + query.getRawQuery());

        IRI dataURI = RefNodeFactory.toIRI(endpoint);

        InputStream inputStream = ResourcesHTTP.asInputStream(dataURI);
        String response = IOUtils.toString(inputStream, StandardCharsets.UTF_8);

        return extractFirstContentId(response);
    }

    private static String extractFirstContentId(String response) throws JsonProcessingException {
        return extractFirstContentId(new ObjectMapper().readTree(response));
    }

    static String extractFirstContentId(JsonNode jsonNode) {
        String contentId = null;
        if (jsonNode.has("results")) {
            JsonNode result = jsonNode.get("results");
            if (result.has("bindings")) {
                for (JsonNode binding : result.get("bindings")) {
                    if (binding.has("contentId")) {
                        JsonNode jsonNode1 = binding.get("contentId");
                        if (jsonNode1.has("value")) {
                            contentId = jsonNode1.get("value").asText();
                            break;
                        }
                    }
                }
            }

        }
        return contentId;
    }


}