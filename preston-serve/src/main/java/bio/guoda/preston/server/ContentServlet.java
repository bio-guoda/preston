package bio.guoda.preston.server;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.CmdGet;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static bio.guoda.preston.server.PropertyNames.PRESTON_PROPERTY_CACHE_ENABLED;
import static bio.guoda.preston.server.PropertyNames.PRESTON_PROPERTY_LOCAL_PATH;
import static bio.guoda.preston.server.PropertyNames.PRESTON_PROPERTY_REMOTE_PATH;
import static bio.guoda.preston.store.HashKeyUtil.isLikelyCompositeHashURI;

public class ContentServlet extends HttpServlet {

    private CmdGet initCmdGet() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setDataDir(getInitParameter(PRESTON_PROPERTY_LOCAL_PATH));
        String remotePath = getInitParameter(PRESTON_PROPERTY_REMOTE_PATH);
        String cacheEnabledValue = getInitParameter(PRESTON_PROPERTY_CACHE_ENABLED);
        cmdGet.setCacheEnabled(StringUtils.equalsIgnoreCase(cacheEnabledValue, "true"));

        if (StringUtils.isNoneBlank(remotePath)) {
            String[] remotes = StringUtils.split(remotePath, ",");
            cmdGet.setRemotes(
                    Arrays.stream(remotes)
                            .map(URI::create)
                            .collect(Collectors.toList()));
        }
        return cmdGet;
    }

    @Override
    public void destroy() {
        log("destroying [" + this.getServletName() + "]");
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws ServletException, IOException {

        log("request [" + request.getRequestURI() + "]");


        String requestURI = RegExUtils
                .replaceFirst(request.getRequestURI(), "^/", "");


        IRI requestIRI = RefNodeFactory.toIRI(requestURI);

        if (!isLikelyCompositeHashURI(requestIRI)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            log("attempting to resolve [" + requestIRI.getIRIString() + "]");
            CmdGet cmdGet = initCmdGet();
            cmdGet.setDisableProgress(true);
            cmdGet.setContentIdsOrAliases(Collections.singletonList(requestIRI));
            cmdGet.setOutputStream(response.getOutputStream());
            try {
                cmdGet.run();
                response.setStatus(HttpServletResponse.SC_OK);
                log("response [" + requestIRI.getIRIString() + "]");
            } catch (Throwable th) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                log("not found [" + requestIRI.getIRIString() + "]");
                throw th;
            }

        }
    }

}