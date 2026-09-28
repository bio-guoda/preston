package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderEppo extends ProcessorReadOnly {
    private static final String EPPO_API_URL_PART = "//api.eppo.int/gd/v2";
    private static final String EPPO_DATASET_REGISTRY_STRING = "https:" + EPPO_API_URL_PART;
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderEppo.class);
    private static final IRI EPPO_REGISTRY = toIRI(EPPO_DATASET_REGISTRY_STRING + "/taxons/list?limit=1000");

    public RegistryReaderEppo(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.OBIS.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream<Quad> nodes = Stream.of(
                    toStatement(Seeds.EPPO, IS_A, ORGANIZATION),
                    toStatement(Seeds.EPPO, DESCRIPTION, toEnglishLiteral("Secretariat of the European and Mediterranean Plant Protection Organization (EPPO).")),
                    toStatement(RegistryReaderEppo.EPPO_REGISTRY, CREATED_BY, Seeds.EPPO),
                    toStatement(RegistryReaderEppo.EPPO_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                    toStatement(RegistryReaderEppo.EPPO_REGISTRY, HAS_VERSION, toBlank())
            );
            ActivityUtil.emitAsNewActivity(nodes, this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(EPPO_API_URL_PART)) {
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
            if (jsonNode.has("data")) {
                for (JsonNode taxon : jsonNode.get("data")) {
                    parseIndividualTaxon(currentPage, emitter, taxon);
                }
            }
            requestRemainingIfNeeded(currentPage, emitter, jsonNode);
        }

    }

    private static void requestRemainingIfNeeded(IRI currentPage, StatementsEmitter emitter, JsonNode jsonNode) {
        JsonNode pagination = jsonNode.at("/pagination");
        if (!pagination.isMissingNode()) {
            JsonNode offset = pagination.at("/offset");
            JsonNode limit = pagination.at("/limit");
            JsonNode total = pagination.at("/total");
            if (offset.isIntegralNumber()
                    && limit.isIntegralNumber()
                    && total.isIntegralNumber()) {
                if (offset.asLong() == 0) {
                    long resultsPerPage = limit.asLong();
                    long remainder = total.asLong() % resultsPerPage;
                    List<Quad> requests = new ArrayList<>();
                    long totalPages = (total.asLong() / resultsPerPage) + (remainder == 0 ? 0 : 1);
                    for (long counter = 1; counter < totalPages * resultsPerPage; counter += resultsPerPage) {
                        IRI taxonPage = toIRI("https:" + EPPO_API_URL_PART + "/taxons/list?" +
                                "limit=" + resultsPerPage +
                                "&offset=" + counter);
                        requests.addAll(Arrays.asList(
                                toStatement(currentPage, HAD_MEMBER, taxonPage),
                                toStatement(taxonPage, HAS_FORMAT, toLiteral(MimeTypes.MIME_TYPE_JSON)),
                                toStatement(taxonPage, HAS_VERSION, toBlank())
                        ));
                    }
                    emitter.emit(requests);
                }
            }
        }
    }

    private static void parseIndividualTaxon(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("eppocode")) {
            String taxonId = result.get("eppocode").asText();
            IRI taxonIri = toIRI("https:" + EPPO_API_URL_PART + "/taxons/taxon/" + taxonId + "/overview");
            emitter.emit(toStatement(currentPage, HAD_MEMBER, taxonIri));
            emitTaxonPage(emitter, taxonIri);
        }
    }

    private static void emitTaxonPage(StatementsEmitter emitter, IRI taxonInfo) {
        emitter.emit(toStatement(taxonInfo, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
        emitter.emit(toStatement(taxonInfo, HAS_VERSION, toBlank()));
    }

}
