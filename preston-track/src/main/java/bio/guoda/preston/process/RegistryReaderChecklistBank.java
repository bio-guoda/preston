package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderChecklistBank extends ProcessorReadOnly {

    public static final String CHECKLIST_BANK_API_DATASET_PART = "//api.checklistbank.org/dataset";
    public static final String CHECKLIST_BANK_DATASET_REGISTRY_STRING = "https:" + CHECKLIST_BANK_API_DATASET_PART;
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderChecklistBank.class);
    public static final IRI CHECKLIST_BANK_REGISTRY = toIRI(CHECKLIST_BANK_DATASET_REGISTRY_STRING);

    public RegistryReaderChecklistBank(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.CHECKLIST_BANK.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            List<Quad> nodes = new ArrayList<>();
            Stream.of(
                    toStatement(Seeds.CHECKLIST_BANK, IS_A, ORGANIZATION),
                    toStatement(
                            RegistryReaderChecklistBank.CHECKLIST_BANK_REGISTRY,
                            DESCRIPTION,
                            toEnglishLiteral("Provides a registry of taxonomic checklist.")
                    )
            ).forEach(nodes::add);

            emitPageRequest(new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, CHECKLIST_BANK_REGISTRY);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(CHECKLIST_BANK_API_DATASET_PART)
                && !getVersionSource(statement).toString().endsWith(".zip")) {
            handleDataset(statement);
        }
    }

    public void handleDataset(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseDatasetResultPage(currentPage, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                }, is, getVersionSource(statement));
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }
        ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
    }

    static void emitNextPage(int offset, int limit, StatementsEmitter emitter, String versionSourceURI) {
        String nextPageURL = versionSourceURI;
        nextPageURL = RegExUtils.replacePattern(nextPageURL, "limit=[0-9]*", "limit=" + limit);
        nextPageURL = RegExUtils.replacePattern(nextPageURL, "offset=[0-9]*", "offset=" + offset);
        nextPageURL = StringUtils.contains(nextPageURL, "?") ? nextPageURL : nextPageURL + "?";
        nextPageURL = StringUtils.contains(nextPageURL, "offset") ? nextPageURL : nextPageURL + "&offset=" + offset;
        nextPageURL = StringUtils.contains(nextPageURL, "limit") ? nextPageURL : nextPageURL + "&limit=" + limit;
        nextPageURL = StringUtils.replace(nextPageURL, "?&", "?");
        IRI nextPage = toIRI(nextPageURL);
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(StatementsEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, CREATED_BY, Seeds.CHECKLIST_BANK),
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parseDatasetResultPage(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("result")) {
                for (JsonNode result : jsonNode.get("result")) {
                    parseIndividualDataset(currentPage, emitter, result);
                }
            } else if (jsonNode.has("key")) {
                parseIndividualDataset(currentPage, emitter, jsonNode);
            } else if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    parseIndividualDataset(currentPage, emitter, node);
                }
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    private static void emitNextPageIfNeeded(StatementsEmitter emitter, IRI versionSource, JsonNode jsonNode) {
        if (!isEndOfRecords(jsonNode)
                && jsonNode.has("total")
                && jsonNode.has("offset")
                && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            if (offset == 0) {
                int totalNumberOfRecords = jsonNode.get("total").asInt();
                int limit = jsonNode.get("limit").asInt();
                String previousURL = versionSource.getIRIString();
                for (int i = limit; i < totalNumberOfRecords; i += limit) {
                    emitNextPage(i, limit, emitter, previousURL);
                }
            }
        }
    }

    private static boolean isEndOfRecords(JsonNode jsonNode) {
        return false;
    }


    public static void parseIndividualDataset(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("key")) {
            String datasetId = result.get("key").asText();
            String datasetIRIString = CHECKLIST_BANK_DATASET_REGISTRY_STRING + "/" + datasetId;
            IRI datasetIRI = toIRI(datasetIRIString);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetIRI));
            if (result.has("doi")) {
                String doi = result.get("doi").asText();
                emitter.emit(toStatement(datasetIRI, SEE_ALSO, toIRI("https://doi.org/" + doi)));
            }
            IRI datasetArchiveCandidate = toIRI(datasetIRIString + "/archive.zip");
            emitter.emit(toStatement(datasetIRI, HAD_MEMBER, datasetArchiveCandidate));
            emitter.emit(toStatement(datasetArchiveCandidate, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_ZIP)));
            emitter.emit(toStatement(datasetArchiveCandidate, HAS_VERSION, toBlank()));

            emitter.emit(toStatement(datasetIRI, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
            emitter.emit(toStatement(
                    datasetIRI,
                    HAS_VERSION,
                    toBlank()));
        }
    }

}
