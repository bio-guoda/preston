package bio.guoda.preston.server;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.CmdGet;
import bio.guoda.preston.store.HashKeyUtil;
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
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static bio.guoda.preston.server.PropertyNames.*;

public class ContentServlet extends HttpServlet {

    private CmdGet initCmdGet() {
        CmdGet cmdGet = new CmdGet();
        cmdGet.setLocalDataDir(getInitParameter(PRESTON_PROPERTY_LOCAL_PATH));
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

        log("got [" + request.getRequestURI() + "]");


        String requestURI = RegExUtils
                .replaceFirst(request.getRequestURI(), "^/", "");

        IRI iri = null;

        IRI hashKey = RefNodeFactory.toIRI(requestURI);
        if (HashKeyUtil.isValidHashKey(hashKey)) {
            iri = HashKeyUtil.extractContentHash(hashKey);
        } else {
            iri = attemptToGuessHashURIFromHexPattern(requestURI, iri);
        }

        if (iri == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            log("lookup [" + iri.getIRIString() + "]");
            CmdGet cmdGet = initCmdGet();
            cmdGet.setDisableProgress(true);
            cmdGet.setContentIdsOrAliases(Collections.singletonList(iri));
            cmdGet.setOutputStream(response.getOutputStream());
            try {
                cmdGet.run();
                response.setStatus(HttpServletResponse.SC_OK);
                log("found [" + iri.getIRIString() + "]");
            } catch (Throwable th) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                log("not found [" + iri.getIRIString() + "]");
                throw th;
            }
        }
    }

    private IRI attemptToGuessHashURIFromHexPattern(String requestURI, IRI iri) {
        for (HashType type : HashType.values()) {
            Matcher matcher = type.getHexPattern().matcher(requestURI);
            if (matcher.matches()) {
                iri = RefNodeFactory.toIRI(type.getPrefix() + requestURI);
                break;
            }
        }
        return iri;
    }
}