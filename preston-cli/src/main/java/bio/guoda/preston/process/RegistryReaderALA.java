package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

public class RegistryReaderALA extends ProcessorReadOnly {
    private static final String ALA_API_URL_PART = "//collections.ala.org.au/ws/dataResource?status=dataAvailable";
    private static final String ALA_DATASET_REGISTRY_STRING = "https:" + ALA_API_URL_PART;
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderALA.class);
    private static final IRI ALA_REGISTRY = toIRI(ALA_DATASET_REGISTRY_STRING);

    public RegistryReaderALA(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.ALA.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            List<Quad> nodes = new ArrayList<Quad>();
            Stream.of(
                    toStatement(Seeds.ALA, IS_A, ORGANIZATION),
                    toStatement(Seeds.ALA, DESCRIPTION, toEnglishLiteral("The Atlas of Living Australia (ALA) is a collaborative, digital, open infrastructure that pulls together Australian biodiversity data from multiple sources, making it accessible and reusable.")),
                    toStatement(RegistryReaderALA.ALA_REGISTRY, CREATED_BY, Seeds.ALA))
                    .forEach(nodes::add);
            emitPageRequest(new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, ALA_REGISTRY);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(ALA_API_URL_PART)) {

            List<Quad> nodes = new ArrayList<Quad>();
            IRI currentPage = (IRI) getVersion(statement);
            try (InputStream is = get(currentPage)) {
                if (is != null) {
                    parse(new StatementsEmitterAdapter() {
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
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement)
                .toString()
                .startsWith("<https://collections.ala.org.au/ws/dataResource/")) {

            List<Quad> nodes = new ArrayList<Quad>();
            IRI currentPage = (IRI) getVersion(statement);
            try (InputStream is = get(currentPage)) {
                if (is != null) {
                    parseResource(new StatementsEmitterAdapter() {
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

    private static void emitPageRequest(StatementsEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, CREATED_BY, Seeds.ALA),
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parse(StatementsEmitter emitter, InputStream in) throws IOException {
        JsonNode registry = new ObjectMapper().readTree(in);
        if (registry != null) {
            for (JsonNode item : registry) {
                if (item.has("uri")) {
                    String resourceURI = item.get("uri").asText();
                    if (StringUtils.isNotBlank(resourceURI)) {
                        emitPageRequest(emitter, toIRI(resourceURI));
                    }
                }
            }
        }

    }

    static void parseResource(StatementsEmitter emitter, InputStream in) throws IOException {
        JsonNode resource = new ObjectMapper().readTree(in);
        if (resource != null) {
            Set<IRI> IRIs = new HashSet<>();
            addIRI(resource, "publicArchiveUrl", IRIs);
            addIRI(resource, "gbifArchiveUrl", IRIs);
            Literal mimeType = null;
            if (resource.has("resourceType")) {
                if (StringUtils.startsWithIgnoreCase(resource.get("resourceType").asText(), "records")) {
                    mimeType = toContentType(MimeTypes.MIME_TYPE_DWCA);
                }
            }
            for (IRI iri : IRIs) {
                if (mimeType != null) {
                    emitter.emit(toStatement(iri, HAS_FORMAT, mimeType));
                }
                emitter.emit(toStatement(iri, HAS_VERSION, toBlank()));
            }
        }

    }

    private static void addIRI(JsonNode resource, String archiveElem, Set<IRI> IRIs) {
        if (resource.has(archiveElem)) {
            String resourceURI = resource.get(archiveElem).asText();
            if (StringUtils.isNotBlank(resourceURI)) {
                IRIs.add(toIRI(resourceURI));
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
