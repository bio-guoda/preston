package bio.guoda.preston.server;

import bio.guoda.preston.MimeTypes;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpHeaders;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class BadgeServlet extends RedirectingServlet {

    @Override
    public void destroy() {
        log("destroying [" + this.getServletName() + "]");
    }


    @Override
    protected void handleRequest(HttpServletResponse response,
                                 String resolverEndpoint,
                                 String sparqlEndpoint,
                                 String queryType,
                                 IRI requestedIdIRI,
                                 int responseHttpStatus,
                                 String resourceType) throws IOException, ServletException {
        try {
            Map<String, String> provInfo = ProvUtil
                    .findMostRecentContentId(
                            requestedIdIRI,
                            queryType,
                            sparqlEndpoint,
                            resourceType
                    );
            final TreeMap<String, String> labelMap = new TreeMap<String, String>() {{
                put(MimeTypes.MIME_TYPE_DWCA, "DwC Archive");
                put(MimeTypes.MIME_TYPE_EML, "EML File");
            }};
            String typeLabel = labelMap.getOrDefault(resourceType, "DwC Archive");
            if (isOfKnownOrigin(provInfo)) {
                URI uri = populateResponseHeader(response, getResolverEndpoint(), provInfo);
                renderTemplate(
                        response,
                        uri,
                        "origin-known.svg",
                        typeLabel
                );
                log("found origin of [" + requestedIdIRI.getIRIString() + "]");
            } else {
                renderTemplate(
                        response,
                        URI.create(getResolverEndpoint() + requestedIdIRI.getIRIString()),
                        "origin-unknown.svg",
                        typeLabel
                );
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                log("did not find origin of [" + requestedIdIRI.getIRIString() + "]");
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

    @Override
    protected String getPrefix() {
        return "/badge/";
    }


    private void renderTemplate(HttpServletResponse response, URI uri, String templateResource, String typeLabel) throws IOException {
        String badgeTemplate = IOUtils.toString(getClass().getResourceAsStream(templateResource), StandardCharsets.UTF_8);
        String badge = StringUtils
                .replace(badgeTemplate, "{{REDIRECT_URL}}", uri.toString())
                .replace("{{TYPE}}", typeLabel);

        try (InputStream inputStream = IOUtils.toInputStream(badge, StandardCharsets.UTF_8)) {
            IOUtils.copy(inputStream, response.getOutputStream());
            response.setHeader(HttpHeaders.CONTENT_TYPE, "image/svg+xml");
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }


}