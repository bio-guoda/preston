package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.TripleLike;

import java.io.IOException;
import java.io.InputStream;
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
    private static final String OBIS_API_URL_PART = "//api.obis.org/v3/dataset";
    private static final String OBIS_DATASET_REGISTRY_STRING = "https:" + OBIS_API_URL_PART;
    private final Log LOG = LogFactory.getLog(RegistryReaderOBIS.class);
    private static final IRI OBIS_REGISTRY = toIRI(OBIS_DATASET_REGISTRY_STRING);

    public RegistryReaderOBIS(BlobStoreReadOnly blobStoreReadOnly, StatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(TripleLike statement) {
        if (Seeds.OBIS.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream.of(
                    toStatement(Seeds.OBIS, IS_A, ORGANIZATION),
                    toStatement(Seeds.OBIS, DESCRIPTION, toEnglishLiteral("OBIS is a global open-access data and information clearing-house on marine biodiversity for science, conservation and sustainable development.")),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, CREATED_BY, Seeds.OBIS),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, HAS_VERSION, toBlank())
            ).forEach(this::emit);
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

    private static void parseIndividualDataset(IRI currentPage, StatementEmitter emitter, JsonNode result) {
        if (result.has("id")) {
            String uuid = result.get("id").asText();
            IRI datasetUUID = fromUUID(uuid);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetUUID));
            if (result.has("archive")) {
                emitArchive(emitter, result, datasetUUID);
            }
        }
    }

    private static void emitArchive(StatementEmitter emitter, JsonNode result, IRI datasetUUID) {
        String urlString = result.get("archive").asText();
        IRI dataArchive = toIRI(urlString);
        emitter.emit(toStatement(datasetUUID, HAD_MEMBER, dataArchive));
        emitter.emit(toStatement(dataArchive, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_DWCA)));
        emitter.emit(toStatement(dataArchive, HAS_VERSION, toBlank()));
    }

}
