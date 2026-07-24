package bio.guoda.preston.process;

import bio.guoda.preston.EnvUtil;
import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.util.AuthContext;
import bio.guoda.preston.util.DryadContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.globalbioticinteractions.doi.DOI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_LABEL;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.ResourcesHTTP.DRYAD_AUTH_TOKEN;

public class RegistryReaderDataDryad extends ProcessorReadOnly {
    public static final String DRYAD_CLIENT_ID = "DRYAD_CLIENT_ID";
    public static final String DRYAD_CLIENT_SECRET = "DRYAD_CLIENT_SECRET";
    private final static Logger LOG = LoggerFactory.getLogger(RegistryReaderDataDryad.class);

    public static final Pattern DATA_DRYAD_DOI_PATTERN
            = Pattern.compile(".*10[.](?<registrantCode>5061)/(?<suffix>dryad[.][a-z0-9]+).*");
    public static final Pattern ENDPOINT_PATTERN = Pattern
            .compile("(?<schema>.*://)(?<host>.*)/(?<path>.*)");

    private AuthContext getAuthContext() {
        return authContext;
    }

    private void setAuthContext(AuthContext authContext) {
        this.authContext = authContext;
    }

    private AuthContext authContext;

    public RegistryReaderDataDryad(BlobStoreReadOnly blobStore, StatementsListener listener) {
        super(blobStore, listener);
    }

    public static void emitDataDryadEndpoint(DOI doi, StatementEmitter emitter) {
        String iriCandidate = doi.toString();
        emitOnDataDryadDoi(emitter, iriCandidate);
    }

    public static void emitOnDataDryadDoi(StatementEmitter emitter, String candidateIRI) {
        Matcher matcher = DATA_DRYAD_DOI_PATTERN.matcher(candidateIRI);
        if (matcher.matches()) {
            String registrantCode = matcher.group("registrantCode");
            String suffix = matcher.group("suffix");
            String versionsEndpoint = "https://datadryad.org/api/v2/datasets/doi%3A" +
                    "10." +
                    registrantCode +
                    "%2F" +
                    suffix +
                    "/versions";
            emitter.emit(toStatement(
                            toIRI(versionsEndpoint),
                            HAS_VERSION,
                            toBlank()
                    )
            );
        }
    }

    public static AuthContext getOrRefreshAuthToken(AuthContext context, Properties properties) throws IOException {
        if (context != null && StringUtils.isNotBlank(context.getAccessToken())) {
            return context;
        } else {
            return getToken(properties);
        }
    }

    private static AuthContext getToken(Properties properties) throws IOException {
        String token = EnvUtil.getEnvironmentVariable(DRYAD_AUTH_TOKEN, getValueOrNull(properties, "dryad.token"));
        if (StringUtils.isNotBlank(token)) {
            return new DryadContext(token);
        } else {
            String url = "https://datadryad.org/oauth/token";
            HttpPost post = new HttpPost(url);
            post.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded;charset=UTF-8");
            String clientId = EnvUtil.getEnvironmentVariable(DRYAD_CLIENT_ID, getValueOrNull(properties, "dryad.client.id"));
            String clientSecret = EnvUtil.getEnvironmentVariable(DRYAD_CLIENT_SECRET, getValueOrNull(properties, "dryad.client.secret"));

            if (StringUtils.isBlank(clientId)) {
                throw new IOException("to authorize with dryad, please set [" + DRYAD_CLIENT_ID + "]");
            }

            if (StringUtils.isBlank(clientSecret)) {
                throw new IOException("to authorize with dryad, please set [" + DRYAD_CLIENT_SECRET + "]");
            }

            UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(
                    Arrays.asList(
                            new BasicNameValuePair("client_id", clientId),
                            new BasicNameValuePair("client_secret", clientSecret),
                            new BasicNameValuePair("grant_type", "client_credentials")
                    )
            );
            post.setEntity(formEntity);

            try (InputStream inputStream = ResourcesHTTP.asInputStream(
                    toIRI(url),
                    post,
                    new DerefProgressLogger(),
                    httpStatusCode -> false
            )) {
                JsonNode tokenConfig = new ObjectMapper().readTree(inputStream);
                return new DryadContext(tokenConfig.at("/access_token").asText());
            }
        }

    }

    private static String getValueOrNull(Properties properties, String key) {
        return properties == null ? null : properties.getProperty(key);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            attemptToParseDatasetVersions(statement, (IRI) getVersion(statement));
        }
    }

    private void attemptToParseDatasetVersions(Quad statement, IRI contentId) {
        final BlankNodeOrIRI subject = statement.getSubject();
        List<Quad> statements = new ArrayList<>();
        StatementEmitter delayedEmitter = new StatementEmitter() {

            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        };
        if (isVersionsEndpoint(subject)) {
            parseVersions(contentId, delayedEmitter);
        } else if (isFilesEndpoint(subject)) {
            parseFiles(contentId, delayedEmitter, (IRI) subject);
        }
        if (!statements.isEmpty()) {
            initAuth();
            ActivityUtil.emitAsNewActivity(statements.stream(), this, statement.getGraphName());
        }
    }

    private void initAuth() {
        try {
            setAuthContext(getOrRefreshAuthToken(getAuthContext(), null));
        } catch (IOException e) {
            LOG.warn("failed to initialize dryad authentication", e);
        }
    }

    private void parseFiles(IRI contentId, StatementEmitter emitter, IRI endpoint) {
        try (InputStream inputStream = get(contentId)) {
            JsonNode jsonNode = new ObjectMapper().readTree(inputStream);
            JsonNode files = jsonNode.at("/_embedded").get("stash:files");
            if (files != null) {
                for (JsonNode file : files) {
                    JsonNode downloadUrl = file.at("/_links/stash:download/href");
                    if (!downloadUrl.isMissingNode()) {
                        String downloadPath = downloadUrl.asText();
                        Matcher matcher = ENDPOINT_PATTERN.
                                matcher(endpoint.getIRIString());
                        if (matcher.matches()) {
                            IRI downloadIRI = toIRI(matcher.group("schema") + matcher.group("host") + downloadPath);
                            JsonNode path = file.at("/path");
                            if (!path.isMissingNode()) {
                                String filename = path.asText();
                                emitter.emit(RefNodeFactory.toStatement(
                                        downloadIRI,
                                        HAS_LABEL,
                                        RefNodeFactory.toLiteral(filename))
                                );
                            }
                            JsonNode type = file.at("/mimeType");
                            if (!type.isMissingNode()) {
                                emitter.emit(RefNodeFactory.toStatement(
                                        downloadIRI,
                                        HAS_FORMAT,
                                        RefNodeFactory.toLiteral(type.asText()))
                                );
                            }
                            if (file.has("digest")
                                    && file.has("digestType")
                                    && StringUtils.equals("sha-256", file.get("digestType").asText())) {
                                String urlString = "hash://sha256/" + file.get("digest").asText();
                                IRI hashKey = toIRI(urlString);
                                if (HashKeyUtil.isValidHashKey(hashKey)) {
                                    emitter.emit(RefNodeFactory.toStatement(
                                            downloadIRI,
                                            HAS_VERSION,
                                            hashKey)
                                    );
                                }
                            }
                            emitter.emit(RefNodeFactory.toStatement(
                                    downloadIRI,
                                    HAS_VERSION,
                                    RefNodeFactory.toBlank())
                            );

                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn("failed to parse versions [" + contentId + "]", e);
        }
    }

    static void parseVersions(IRI parent, StatementEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        JsonNode versions = r.at("/_embedded/stash:versions");
        if (!versions.isMissingNode() && versions.isArray()) {
            for (JsonNode version : versions) {
                JsonNode filesEndpoint = version.at("/_links/stash:files/href");
                if (!filesEndpoint.isMissingNode()) {
                    IRI fileEndpoint = RefNodeFactory.toIRI("https://datadryad.org" + filesEndpoint.asText());
                    emitter.emit(toStatement(parent, HAD_MEMBER, fileEndpoint));
                    emitter.emit(toStatement(fileEndpoint, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                    emitter.emit(toStatement(fileEndpoint, HAS_VERSION, toBlank()));
                }
            }
        }
    }


    private void parseVersions(IRI refNode, StatementEmitter emitter) {
        try {
            InputStream is = get(refNode);
            if (is != null) {
                parseVersions(refNode, emitter, is);
            }
        } catch (IOException e) {
            LOG.warn("failed to parse publishers [" + refNode.toString() + "]", e);
        }
    }

    public static boolean isVersionsEndpoint(BlankNodeOrIRI subject) {
        String suspectedDataDryadURI = subject.ntriplesString();
        return StringUtils.startsWith(suspectedDataDryadURI, "<https://datadryad.org/api/v2/datasets/")
                && StringUtils.endsWith(subject.ntriplesString(), "/versions>");
    }

    public static boolean isFilesEndpoint(BlankNodeOrIRI subject) {
        String suspectedDataDryadURI = subject.ntriplesString();
        return StringUtils.startsWith(suspectedDataDryadURI, "<https://datadryad.org/api/v2/versions/")
                && StringUtils.endsWith(suspectedDataDryadURI, "/files>");
    }


}
