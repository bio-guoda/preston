package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderOBIS extends ProcessorReadOnly {
    private static final String OBIS_API_URL_PART = "//api.obis.org/v3/dataset";
    private static final String OBIS_DATASET_REGISTRY_STRING = "https:" + OBIS_API_URL_PART;
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderOBIS.class);
    private static final IRI OBIS_REGISTRY = toIRI(OBIS_DATASET_REGISTRY_STRING);

    public RegistryReaderOBIS(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.OBIS.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream<Quad> nodes = Stream.of(
                    toStatement(Seeds.OBIS, IS_A, ORGANIZATION),
                    toStatement(Seeds.OBIS, DESCRIPTION, toEnglishLiteral("OBIS is a global open-access data and information clearing-house on marine biodiversity for science, conservation and sustainable development.")),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, CREATED_BY, Seeds.OBIS),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(RegistryReaderOBIS.OBIS_REGISTRY, HAS_VERSION, toBlank())
            );
            ActivityUtil.emitAsNewActivity(nodes, this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(OBIS_API_URL_PART)) {
            List<Quad> nodes = new ArrayList<>();
            try {
                IRI currentPage = (IRI) getVersion(statement);
                InputStream is = get(currentPage);
                if (is != null) {
                    parse(currentPage, new StatementsEmitterAdapter() {
                        @Override
                        public void emit(Quad statement) {
                            nodes.add(statement);
                        }
                    }, is);
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    static void parse(IRI currentPage, StatementsEmitter emitter, InputStream in) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("results")) {
                for (JsonNode result : jsonNode.get("results")) {
                    parseIndividualDataset(currentPage, emitter, result);
                }
            }
        }

    }

    private static void parseIndividualDataset(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("id")) {
            String uuid = result.get("id").asText();
            IRI datasetUUID = toIRI(uuid);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetUUID));
            if (result.has("archive")) {
                emitArchive(emitter, result, datasetUUID);
            }
        }
    }

    private static void emitArchive(StatementsEmitter emitter, JsonNode result, IRI datasetUUID) {
        String urlString = result.get("archive").asText();
        IRI dataArchive = toIRI(urlString);
        emitter.emit(toStatement(datasetUUID, HAD_MEMBER, dataArchive));
        emitter.emit(toStatement(dataArchive, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_DWCA)));
        emitter.emit(toStatement(dataArchive, HAS_VERSION, toBlank()));
    }

}
