package bio.guoda.preston.server;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.HashKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpHeaders;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static bio.guoda.preston.server.PropertyNames.PRESTON_CONTENT_RESOLVER_ENDPONT;
import static bio.guoda.preston.server.PropertyNames.PRESTON_SPARQL_ENDPONT;

public class RedirectingServlet extends HttpServlet {

    public static final String SEEN_AT = "seenAt";
    public static final String PROVENANCE_ID = "provenanceId";
    public static final String ARCHIVE_URL = "archiveUrl";
    public static final String UUID = "uuid";
    public static final String DOI = "doi";
    public static final String CONTENT_ID = "contentId";
    public static final String ACTIVITY = "activity";

    @Override
    public void destroy() {
        log("destroying [" + this.getServletName() + "]");
    }


    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        String resolverEndpoint = getResolverEndpoint();
        String sparqlEndpoint = getInitParameter(PRESTON_SPARQL_ENDPONT);

        String requestedId = parseRequestedIdOrThrow(request.getRequestURI(), getPrefix());

        String queryType = ProvUtil.queryTypeForRequestedId(requestedId);

        if (StringUtils.equals(queryType, ProvUtil.QUERY_TYPE_DOI)) {
            requestedId = "https://doi.org/" + requestedId;
        }

        IRI requestedIdIRI = RefNodeFactory.toIRI(requestedId);

        if (StringUtils.equals("unknown", queryType)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            log("attempting to direct [" + requestedId + "] as [" + queryType + "]");

            handleRequest(
                    response,
                    resolverEndpoint,
                    sparqlEndpoint,
                    queryType,
                    requestedIdIRI,
                    redirectOnGetRequest(request));

        }
    }

    protected String getPrefix() {
        return "/";
    }

    protected String getResolverEndpoint() {
        return getInitParameter(PRESTON_CONTENT_RESOLVER_ENDPONT);
    }

    private int redirectOnGetRequest(HttpServletRequest request) {
        return StringUtils.equals(request.getMethod(), "HEAD")
                ? HttpServletResponse.SC_OK
                : HttpServletResponse.SC_MOVED_TEMPORARILY;
    }

    protected void handleRequest(HttpServletResponse response,
                                 String resolverEndpoint,
                                 String sparqlEndpoint,
                                 String queryType,
                                 IRI requestedIdIRI,
                                 int responseHttpStatus) throws IOException, ServletException {
        try {
            Map<String, String> provInfo = ProvUtil.findMostRecentContentId(
                    requestedIdIRI,
                    queryType,
                    sparqlEndpoint);
            if (isOfKnownOrigin(provInfo)) {
                populateResponseHeader(response, resolverEndpoint, provInfo);
                response.setStatus(responseHttpStatus);
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

    protected URI populateResponseHeader(HttpServletResponse response, String resolverEndpoint, Map<String, String> provInfo) {
        String contentId = provInfo.get(CONTENT_ID);
        URI uri = getResolverURI(resolverEndpoint, contentId);
        response.setHeader(HttpHeaders.LOCATION, uri.toString());
        response.setHeader(HttpHeaders.CONTENT_TYPE, MimeTypes.MIME_TYPE_DWCA);
        response.setHeader(HttpHeaders.ETAG, contentId);
        response.setHeader(HttpHeaders.CONTENT_LOCATION, provInfo.get(ARCHIVE_URL));
        List<String> influencedBy = new ArrayList<>();
        String uuid = provInfo.get(UUID);
        response.setHeader("X-UUID", uuid);
        influencedBy.add(uuid);
        String doi = provInfo.get(DOI);
        if (StringUtils.isNotBlank(doi)) {
            response.setHeader("X-DOI", doi);
            influencedBy.add(doi);
        }
        response.setHeader("X-PROV", provInfo.get(PROVENANCE_ID));
        response.setHeader("X-PROV-wasInfluencedBy", StringUtils.join(influencedBy, " "));
        response.setHeader("X-PROV-wasGeneratedBy", provInfo.get(ACTIVITY));
        response.setHeader("X-PROV-generatedAtTime", provInfo.get(SEEN_AT));
        response.setHeader("X-PAV-hasVersion", provInfo.get(CONTENT_ID));
        return uri;
    }

    protected URI getResolverURI(String resolverEndpoint, String contentId) {
        return HashKeyUtil.insertSlashIfNeeded(URI.create(resolverEndpoint), contentId);
    }

    protected boolean isOfKnownOrigin(Map<String, String> provInfo) {
        return provInfo.containsKey(CONTENT_ID) && StringUtils.isNotBlank(provInfo.get(CONTENT_ID));
    }

    private String parseRequestedIdOrThrow(String requestURI, String prefix) throws ServletException {
        log("request [" + requestURI + "]");

        return Stream.of(requestURI).filter(req -> StringUtils.startsWith(req, prefix))
                .map(req -> StringUtils.substring(req, prefix.length()))
                .findFirst()
                .orElseThrow(() -> new ServletException("invalid request [" + requestURI + "]"));
    }


}