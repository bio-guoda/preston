package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TaxonWorksJSONStreamHandler implements ContentStreamHandler {
    private final Logger LOG = LoggerFactory.getLogger(TaxonWorksJSONStreamHandler.class);

    private static final String TAXON_ID_SUFFIX = "_taxon_id";
    private static final String TARGET = "target";
    private static final String SOURCE = "source";
    private static final String BIOLOGICAL_ASSOCIATION_SUBJECT_ID = "biological_association_subject_id";
    private static final String BIOLOGICAL_ASSOCIATION_OBJECT_ID = "biological_association_object_id";
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private final Map<String, Map<Long, List<ObjectNode>>> requestedIds;
    private static final String TAXON_ROOTS_RESOLVED = "taxonRootsResolved";
    private static final String REFERENCE_RESOLVED = "referenceResolved";

    public TaxonWorksJSONStreamHandler(ContentStreamHandler contentStreamHandler,
                                       OutputStream os,
                                       Map<String, Map<Long, List<ObjectNode>>> requestedIds) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.requestedIds = requestedIds;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                try {
                    JsonNode jsonNode = new ObjectMapper().readTree(is);
                    if (jsonNode.isArray()) {
                        for (JsonNode node : jsonNode) {
                            handleNodeFor(version, node);
                        }
                    } else {
                        handleNodeFor(version, jsonNode);
                    }
                } catch (JsonProcessingException ex) {
                    // ignore assumed malformed json
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("cannot handle non-github metadata JSON", e);
        }
        return foundAtLeastOne.get();
    }

    private void handleNodeFor(IRI version, JsonNode jsonNode) throws IOException {
        List<ObjectNode> resolvedNodes = new ArrayList<>();
        if (isCitation(jsonNode)) {
            ObjectNode objectNode = new ObjectMapper().createObjectNode();
            objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(version.getIRIString()));
            objectNode.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf("application/vnd.taxonworks+json"));
            Long sourceId = jsonNode.get("source_id").asLong();
            registerIdForNode(sourceId, objectNode, "source_id");
            long citationObjectId = jsonNode.get("citation_object_id").asLong();
            registerIdForNode(citationObjectId, objectNode, "citation_object_id");
            objectNode.set("referenceId", new TextNode("https://sfg.taxonworks.org/api/v1/sources/" + sourceId));
            objectNode.set("interactionId", new TextNode("https://sfg.taxonworks.org/api/v1/biological_associations/" + citationObjectId));
            objectNode.set(TAXON_ROOTS_RESOLVED, IntNode.valueOf(0));
            objectNode.set(REFERENCE_RESOLVED, BooleanNode.valueOf(false));
        } else if (isSource(jsonNode)) {
            resolveId(jsonNode, "source_id", new Consumer<ObjectNode>() {
                @Override
                public void accept(ObjectNode resolved) {
                    resolved.set("referenceCitation", new TextNode(jsonNode.get("bibtex").asText()));
                    resolved.set(REFERENCE_RESOLVED, BooleanNode.valueOf(true));
                    resolvedNodes.add(resolved);
                }
            });
        } else if (isAssociation(jsonNode)) {
            resolveId(jsonNode, "citation_object_id", new Consumer<ObjectNode>() {
                @Override
                public void accept(ObjectNode resolved) {
                    resolved.set("interactionTypeId", TextNode.valueOf("gid://taxon-works/BiologicalRelationship/" + jsonNode.get("biological_relationship_id").asText()));
                    if (jsonNode.has("biological_relationship")) {
                        JsonNode rel = jsonNode.get("biological_relationship");
                        if (rel.has("object_tag")) {
                            resolved.set("interactionTypeName", TextNode.valueOf(rel.get("object_tag").asText()));
                        }
                    }
                    registerIdForNode(jsonNode.get(BIOLOGICAL_ASSOCIATION_SUBJECT_ID).asLong(), resolved, BIOLOGICAL_ASSOCIATION_SUBJECT_ID);
                    registerIdForNode(jsonNode.get(BIOLOGICAL_ASSOCIATION_OBJECT_ID).asLong(), resolved, BIOLOGICAL_ASSOCIATION_OBJECT_ID);
                    resolvedNodes.add(resolved);
                }
            });
        } else {
            final String TARGET_TAXON_ID = TARGET + TAXON_ID_SUFFIX;
            final String SOURCE_TAXON_ID = SOURCE + TAXON_ID_SUFFIX;
            if (isOTU(jsonNode)) {
                resolveId(jsonNode, BIOLOGICAL_ASSOCIATION_SUBJECT_ID, new Consumer<ObjectNode>() {
                    @Override
                    public void accept(ObjectNode resolvedSubject) {
                        registerIdForNode(jsonNode.get("taxon_name_id").asLong(), resolvedSubject, SOURCE_TAXON_ID);
                        resolvedNodes.add(resolvedSubject);
                    }
                });

                resolveId(jsonNode, BIOLOGICAL_ASSOCIATION_OBJECT_ID, new Consumer<ObjectNode>() {
                    @Override
                    public void accept(ObjectNode resolvedObject) {
                        registerIdForNode(jsonNode.get("taxon_name_id").asLong(), resolvedObject, TARGET_TAXON_ID);
                        resolvedNodes.add(resolvedObject);
                    }
                });
            } else if (isName(jsonNode)) {
                applyName(jsonNode, resolvedNodes, TARGET);
                applyName(jsonNode, resolvedNodes, SOURCE);
            }
        }
        resolvedNodes.forEach(this::notifyResolution);
    }

    private void applyName(JsonNode jsonNode, List<ObjectNode> resolvedNodes, String nameRole) {
        resolveName(jsonNode, nameRole + TAXON_ID_SUFFIX, new Consumer<ObjectNode>() {

            @Override
            public void accept(ObjectNode resolvedNode) {
                String taxonName = jsonNode.get("cached").asText();
                String taxonId = "gid://taxon-works/TaxonName/" + jsonNode.get("id").asText();
                String taxonRank = jsonNode.hasNonNull("rank") ? jsonNode.get("rank").asText() : "";
                String taxonAuthorship = jsonNode.hasNonNull("cached_author_year") ? jsonNode.get("cached_author_year").asText() : "";
                if (!resolvedNode.has(nameRole + "TaxonName")) {
                    resolvedNode.set(nameRole + "TaxonName", TextNode.valueOf(taxonName));
                    resolvedNode.set(nameRole + "TaxonId", TextNode.valueOf(taxonId));
                    resolvedNode.set(nameRole + "TaxonRank", TextNode.valueOf(taxonRank));
                    resolvedNode.set(nameRole + "TaxonAuthorship", TextNode.valueOf(taxonAuthorship));
                }

                appendPath(resolvedNode, taxonName, nameRole + "TaxonPath");
                appendPath(resolvedNode, taxonId, nameRole + "TaxonPathIds");
                appendPath(resolvedNode, taxonRank, nameRole + "TaxonPathNames");

                resolvedNodes.add(resolvedNode);

            }
        });
    }

    private void appendPath(ObjectNode resolvedNode, String taxonName, String fieldName) {
        String prefix = "";
        if (resolvedNode.has(fieldName)) {
            prefix = resolvedNode.get(fieldName).asText();
        }
        resolvedNode.set(fieldName, TextNode.valueOf(taxonName + (StringUtils.isBlank(prefix) ? "" : " | " + prefix)));
    }

    private void resolveName(JsonNode jsonNode, String idType, Consumer<ObjectNode> consumer) {
        resolveId(jsonNode, idType, new Consumer<ObjectNode>() {
            @Override
            public void accept(ObjectNode objectNode) {
                if (jsonNode.has("parent_id")) {
                    if (jsonNode.hasNonNull("parent_id")) {
                        registerIdForNode(jsonNode.get("parent_id").asLong(), objectNode, idType);
                    } else {
                        incrementCounter(objectNode, TAXON_ROOTS_RESOLVED);
                    }
                }
                consumer.accept(objectNode);
            }
        });
    }

    private void incrementCounter(ObjectNode objectNode, String counterName) {
        int counterValue = objectNode.get(counterName).intValue();
        objectNode.set(counterName, IntNode.valueOf(counterValue + 1));
    }

    private boolean isName(JsonNode jsonNode) {
        return jsonNode.has("nomenclatural_code");
    }

    private boolean isOTU(JsonNode jsonNode) {
        return jsonNode.has("global_id")
                && StringUtils.startsWith(
                jsonNode.get("global_id").asText(),
                "gid://taxon-works/Otu"
        );
    }

    private boolean isAssociation(JsonNode jsonNode) {
        return jsonNode.has("biological_relationship_id");
    }

    private void resolveId(JsonNode id, String idType, Consumer<ObjectNode> resolvedListener) {
        Map<Long, List<ObjectNode>> idMap = requestedIds.get(idType);
        if (idMap != null) {
            List<ObjectNode> objectNodes = idMap.remove(id.get("id").asLong());
            if (objectNodes != null) {
                objectNodes.forEach(resolvedListener);
            }
        }
    }

    private void notifyResolution(ObjectNode resolvedNode) {
        if (discoveredTaxonRoots(resolvedNode)
                && hasResolvedReference(resolvedNode)) {
            try {
                IOUtils.copy(IOUtils.toInputStream(resolvedNode.toString(), StandardCharsets.UTF_8), outputStream);
                IOUtils.write("\n", outputStream, StandardCharsets.UTF_8);
            } catch (IOException e) {
                // ignore for now
            }
        }
    }

    private boolean discoveredTaxonRoots(ObjectNode resolvedNode) {
        int taxonRoots = resolvedNode.has(TAXON_ROOTS_RESOLVED)
                ? resolvedNode.get(TAXON_ROOTS_RESOLVED).intValue() : 0;
        return taxonRoots >= 2;
    }

    private boolean hasResolvedReference(ObjectNode resolvedNode) {
        return resolvedNode.has(REFERENCE_RESOLVED)
                && resolvedNode.get(REFERENCE_RESOLVED).booleanValue();
    }

    private boolean isSource(JsonNode jsonNode) {
        JsonNode baseClass = jsonNode.get("base_class");
        return baseClass != null && StringUtils.equals(baseClass.asText(), "Source");
    }

    private void registerIdForNode(long id, ObjectNode node, String idType) {
        Map<Long, List<ObjectNode>> typeMap = requestedIds.containsKey(idType)
                ? requestedIds.get(idType)
                : new LRUMap<Long, List<ObjectNode>>(10000, 10000) {
            @Override
            protected boolean removeLRU(LinkEntry entry) {
                LOG.warn("evicting entry: [" + entry.getKey() + "]: ");
                return super.removeLRU(entry);
            }
        };
        List<ObjectNode> objectNodes = typeMap.containsKey(id) ? typeMap.get(id) : new ArrayList<>();
        objectNodes.add(node);
        typeMap.put(id, objectNodes);
        requestedIds.put(idType, typeMap);
    }

    private boolean isCitation(JsonNode jsonNode) {
        return jsonNode.has("citation_object_id")
                && jsonNode.has("citation_object_type")
                && StringUtils.equals("BiologicalAssociation", jsonNode.get("citation_object_type").asText(""))
                && jsonNode.has("source_id");
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
