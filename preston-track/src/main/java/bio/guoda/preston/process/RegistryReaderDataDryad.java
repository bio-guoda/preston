package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.util.ResultPagerUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

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


    public RegistryReaderDataDryad(BlobStoreReadOnly blobStore, StatementsListener listener) {
        super(blobStore, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            attemptToParseDatasetVersions(statement, (IRI) getVersion(statement));
        }
    }

    private void attemptToParseDatasetVersions(Quad statement, IRI toBeParsed) {
        final BlankNodeOrIRI subject = statement.getSubject();
        if (isVersionsEndpoint(subject)) {
            parseVersions(toBeParsed, this);
        }
    }

    static void parseVersions(IRI parent, StatementsEmitter emitter, InputStream is) throws IOException {
        JsonNode r = new ObjectMapper().readTree(is);
        JsonNode versions = r.at("/_embedded/stash:versions");
        if (!versions.isMissingNode() && versions.isArray()) {
            for (JsonNode version : versions) {
                JsonNode filesEndpoint = version.at("/_links/stash:files/href");
                if (!filesEndpoint.isMissingNode()) {
                    IRI refNodeFeed = RefNodeFactory.toIRI("https://datadryad.org/" + filesEndpoint.asText());
                    emitter.emit(toStatement(parent, HAD_MEMBER, refNodeFeed));
                    emitter.emit(toStatement(refNodeFeed, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                    emitter.emit(toStatement(refNodeFeed, HAS_VERSION, toBlank()));
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


}
