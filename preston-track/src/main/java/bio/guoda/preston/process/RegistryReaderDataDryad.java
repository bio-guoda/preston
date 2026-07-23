package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.globalbioticinteractions.doi.DOI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderDataDryad extends ProcessorReadOnly {
    private final static Logger LOG = LoggerFactory.getLogger(RegistryReaderDataDryad.class);

    public static final Pattern DATA_DRYAD_DOI_PATTERN
            = Pattern.compile(".*10[.](?<registrantCode>5061)/(?<suffix>dryad[.][a-z0-9]+).*");
    public static final Pattern ENDPOINT_PATTERN = Pattern
            .compile("(?<schema>.*://)(?<host>.*/)(?<path>.*)");

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

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            attemptToParseDatasetVersions(statement, (IRI) getVersion(statement));
        }
    }

    private void attemptToParseDatasetVersions(Quad statement, IRI contentId) {
        final BlankNodeOrIRI subject = statement.getSubject();
        if (isVersionsEndpoint(subject)) {
            parseVersions(contentId, this);
        } else if (isFilesEndpoint(subject)) {
            parseFiles(contentId, this, (IRI) subject);
        }
    }

    private void parseFiles(IRI contentId, StatementsEmitter emitter, IRI endpoint) {
        try (InputStream inputStream = get(contentId)) {
            JsonNode jsonNode = new ObjectMapper().readTree(inputStream);
            JsonNode files = jsonNode.at("/_embedded").get("stash:files");
            if (files != null) {
                for (JsonNode file : files) {
                    JsonNode downloadUrl = file.at("/_links/stash:download/href");
                    if (!downloadUrl.isMissingNode()) {
                        String relativeUrl = downloadUrl.asText();
                        Matcher matcher = ENDPOINT_PATTERN.
                                matcher(endpoint.getIRIString());
                        if (matcher.matches()) {
                            emitter.emit(RefNodeFactory.toStatement(
                                    RefNodeFactory.toIRI(matcher.group("schema") + matcher.group("host") + relativeUrl),
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

    static void parseVersions(IRI parent, StatementsEmitter emitter, InputStream is) throws IOException {
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


    private void parseVersions(IRI refNode, StatementsEmitter emitter) {
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
