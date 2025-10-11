package bio.guoda.preston.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

public class HistoryServlet extends RedirectingServlet {

    protected void handleRequest(HttpServletResponse response,
                                 String resolverEndpoint,
                                 String sparqlEndpoint,
                                 String queryType,
                                 IRI requestedIdIRI,
                                 int responseHttpStatus,
                                 String contentType) throws IOException, ServletException {
        try {
            int maxResults = 1024;
            String result = ProvUtil.findProvenance(requestedIdIRI, queryType, sparqlEndpoint, contentType, getProvenanceId(), false, maxResults);
            ProvUtil.extractProvenanceInfo(new ObjectMapper().readTree(result), new Consumer<JsonNode>() {
                @Override
                public void accept(JsonNode jsonNode) {
                    InputStream is = IOUtils.toInputStream(jsonNode.toString() + "\n", StandardCharsets.UTF_8);
                    try {
                        IOUtils.copy(is, response.getOutputStream());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, maxResults);
        } catch (URISyntaxException e) {
            throw new ServletException("failed to retrieve history for [" + requestedIdIRI.getIRIString() + "]");
        }


    };

}