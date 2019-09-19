package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.fromUUID;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class RegistryReaderOBIS extends ProcessorReadOnly {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("DWC_ARCHIVE", MimeTypes.MIME_TYPE_DWCA);
        put("EML", MimeTypes.MIME_TYPE_EML);
    }};


    public static final String OBIS_API_URL_PART = "//api.obis.org/v3/dataset";
    public static final String OBIS_DATASET_REGISTRY_STRING = "https:" + OBIS_API_URL_PART;
    private final Log LOG = LogFactory.getLog(RegistryReaderOBIS.class);
    public static final IRI OBIS_REGISTRY = toIRI(OBIS_DATASET_REGISTRY_STRING);

    public RegistryReaderOBIS(BlobStoreReadOnly blobStoreReadOnly, StatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Triple statement) {
        if (Seeds.OBIS.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream.of(
                    toStatement(Seeds.OBIS, IS_A, ORGANIZATION),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, DESCRIPTION, toEnglishLiteral("OBIS is a global open-access data and information clearing-house on marine biodiversity for science, conservation and sustainable development.")),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, CREATED_BY, Seeds.OBIS))
                    .forEach(this::emit);
            emitPageRequest(this, OBIS_REGISTRY);
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(OBIS_API_URL_PART)) {
            try {
                IRI currentPage = (IRI) getVersion(statement);
                InputStream is = get(currentPage);
                if (is != null) {
                    parse(currentPage, this, is);
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
        }
    }

    static void emitNextPage(int offset, int limit, StatementEmitter emitter, String versionSourceURI) {
        String nextPageURL = versionSourceURI;
        nextPageURL = StringUtils.replacePattern(nextPageURL, "limit=[0-9]*", "limit=" + limit);
        nextPageURL = StringUtils.replacePattern(nextPageURL, "offset=[0-9]*", "offset=" + offset);
        nextPageURL = StringUtils.contains(nextPageURL, "?") ? nextPageURL : nextPageURL + "?";
        nextPageURL = StringUtils.contains(nextPageURL, "offset") ? nextPageURL : nextPageURL + "&offset=" + offset;
        nextPageURL = StringUtils.contains(nextPageURL, "limit") ? nextPageURL : nextPageURL + "&limit=" + limit;
        nextPageURL = StringUtils.replace(nextPageURL, "?&", "?");
        IRI nextPage = toIRI(nextPageURL);
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(StatementEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parse(IRI currentPage, StatementEmitter emitter, InputStream in) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("results")) {
                for (JsonNode result : jsonNode.get("results")) {
                    parseIndividualDataset(currentPage, emitter, result);
                }
            }
        }

    }

    public static void parseIndividualDataset(IRI currentPage, StatementEmitter emitter, JsonNode result) {
        if (result.has("id")) {
            String uuid = result.get("id").asText();
            IRI datasetUUID = fromUUID(uuid);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetUUID));
            if (result.has("archive")) {
                emitArchive(emitter, result, datasetUUID);
            }
        }
    }

    public static void emitArchive(StatementEmitter emitter, JsonNode result, IRI datasetUUID) {
        String urlString = result.get("archive").asText();
        IRI dataArchive = toIRI(urlString);
        emitter.emit(toStatement(datasetUUID, HAD_MEMBER, dataArchive));
        emitter.emit(toStatement(dataArchive, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_DWCA)));
        emitter.emit(toStatement(dataArchive, HAS_VERSION, toBlank()));
    }

}
