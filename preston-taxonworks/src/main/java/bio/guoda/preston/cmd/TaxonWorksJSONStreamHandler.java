package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaxonWorksJSONStreamHandler implements ContentStreamHandler {

    public static final String TAXON_ID_SUFFIX = "_taxon_id";
    public static final String TARGET = "target";
    public static final String SOURCE = "source";
    public static final String BIOLOGICAL_ASSOCIATION_SUBJECT_ID = "biological_association_subject_id";
    public static final String BIOLOGICAL_ASSOCIATION_OBJECT_ID = "biological_association_object_id";
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private LinkedList<ObjectNode> nodes = new LinkedList<>();
    private final Map<String, Map<Long, ObjectNode>> requestedIds;
    public static final String UNRESOLVED_REFERENCE_COUNT = "unresolvedReferenceCount";

    public TaxonWorksJSONStreamHandler(ContentStreamHandler contentStreamHandler,
                                       OutputStream os,
                                       Map<String, Map<Long, ObjectNode>> requestedIds) {
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
            registerIdForNode(jsonNode.get("source_id").asLong(), objectNode, "source_id");
            registerIdForNode(jsonNode.get("citation_object_id").asLong(), objectNode, "citation_object_id");
        } else if (isSource(jsonNode)) {
            ObjectNode resolved = resolveId(jsonNode, "source_id");
            if (resolved != null) {
                resolved.set("referenceCitation", new TextNode(jsonNode.get("object_label").asText()));
                resolved.set("referenceId", new TextNode(jsonNode.get("global_id").asText()));
                resolvedNodes.add(resolved);
            }
        } else if (isAssociation(jsonNode)) {
            ObjectNode resolved = resolveId(jsonNode, "citation_object_id");
            if (resolved != null) {
                resolved.set("interactionTypeId", new TextNode("gid://taxon-works/BiologicalRelationship/" + jsonNode.get("biological_relationship_id").asText()));
                registerIdForNode(jsonNode.get(BIOLOGICAL_ASSOCIATION_SUBJECT_ID).asLong(), resolved, BIOLOGICAL_ASSOCIATION_SUBJECT_ID);
                registerIdForNode(jsonNode.get(BIOLOGICAL_ASSOCIATION_OBJECT_ID).asLong(), resolved, BIOLOGICAL_ASSOCIATION_OBJECT_ID);
                resolvedNodes.add(resolved);
            }
        } else {
            final String TARGET_TAXON_ID = TARGET + TAXON_ID_SUFFIX;
            final String SOURCE_TAXON_ID = SOURCE + TAXON_ID_SUFFIX;
            if (isOTU(jsonNode)) {
                ObjectNode resolvedSubject = resolveId(jsonNode, BIOLOGICAL_ASSOCIATION_SUBJECT_ID);
                if (resolvedSubject != null) {
                    registerIdForNode(jsonNode.get("taxon_name_id").asLong(), resolvedSubject, SOURCE_TAXON_ID);
                    resolvedNodes.add(resolvedSubject);
                }

                ObjectNode resolvedObject = resolveId(jsonNode, BIOLOGICAL_ASSOCIATION_OBJECT_ID);
                if (resolvedObject != null) {
                    registerIdForNode(jsonNode.get("taxon_name_id").asLong(), resolvedObject, TARGET_TAXON_ID);
                    resolvedNodes.add(resolvedObject);
                }
            } else if (isName(jsonNode)) {
                applyName(jsonNode, resolvedNodes, TARGET);
                applyName(jsonNode, resolvedNodes, SOURCE);
            }
        }
        resolvedNodes.forEach(this::notifyResolution);
    }

    private void applyName(JsonNode jsonNode, List<ObjectNode> resolvedNodes, String nameRole) {
        ObjectNode resolvedNode = resolveName(jsonNode, nameRole + TAXON_ID_SUFFIX);
        if (resolvedNode != null) {
            String taxonName = jsonNode.get("name").asText();
            String taxonId = "gid://taxon-works/TaxonName/" + jsonNode.get("id").asText();
            String taxonRank = jsonNode.get("rank").asText();
            if (!resolvedNode.has(nameRole + "TaxonName")) {
                resolvedNode.set(nameRole + "TaxonName", TextNode.valueOf(taxonName));
                resolvedNode.set(nameRole + "TaxonId", TextNode.valueOf(taxonId));
                resolvedNode.set(nameRole + "TaxonRank", TextNode.valueOf(taxonRank));
            }

            appendPath(resolvedNode, taxonName, nameRole + "TaxonPath");
            appendPath(resolvedNode, taxonId, nameRole + "TaxonPathIds");
            appendPath(resolvedNode, taxonRank, nameRole + "TaxonPathNames");

            resolvedNodes.add(resolvedNode);
        }
    }

    private void appendPath(ObjectNode resolvedNode, String taxonName, String fieldName) {
        String prefix = "";
        if (resolvedNode.has(fieldName)) {
            prefix = resolvedNode.get(fieldName).asText();
        }
        resolvedNode.set(fieldName, TextNode.valueOf(taxonName + (StringUtils.isBlank(prefix) ? "" : " | " + prefix)));
    }

    private ObjectNode resolveName(JsonNode jsonNode, String idType) {
        ObjectNode objectNode = resolveId(jsonNode, idType);
        if (objectNode != null
                && jsonNode.has("parent_id")
                && jsonNode.hasNonNull("parent_id")) {
            registerIdForNode(jsonNode.get("parent_id").asLong(), objectNode, idType);
        }
        return objectNode;
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

    private ObjectNode resolveId(JsonNode id, String idType) {
        ObjectNode resolvedNode = null;
        Map<Long, ObjectNode> idMap = requestedIds.get(idType);
        if (idMap != null) {
            ObjectNode jsonNode = idMap.get(id.get("id").asLong());
            if (jsonNode != null) {
                resolvedNode = jsonNode;
            }
        }
        return resolvedNode;
    }

    private void notifyResolution(ObjectNode resolvedNode) {
        int unresolvedReferencesCount = resolvedNode.get(UNRESOLVED_REFERENCE_COUNT).intValue();
        if (moreReferencesLeftResolving(resolvedNode)) {
            resolvedNode.set(UNRESOLVED_REFERENCE_COUNT, IntNode.valueOf(unresolvedReferencesCount - 1));
        } else {
            try {
                IOUtils.copy(IOUtils.toInputStream(resolvedNode.toString(), StandardCharsets.UTF_8), outputStream);
                IOUtils.write("\n", outputStream, StandardCharsets.UTF_8);
            } catch (IOException e) {
                // ignore for now
            }
        }
    }

    private boolean moreReferencesLeftResolving(ObjectNode resolvedNode) {
        return !resolvedNode.has("unresolvedReferenceCount") || resolvedNode.get("unresolvedReferenceCount").intValue() > 1;
    }

    private boolean isSource(JsonNode jsonNode) {
        JsonNode baseClass = jsonNode.get("base_class");
        return baseClass != null && StringUtils.equals(baseClass.asText(), "Source");
    }

    private void registerIdForNode(long id, ObjectNode node, String idType) {
        Map<Long, ObjectNode> typeMap = requestedIds.containsKey(idType)
                ? requestedIds.get(idType)
                : new TreeMap<>();
        typeMap.put(id, node);
        requestedIds.put(idType, typeMap);
        if (node.has(UNRESOLVED_REFERENCE_COUNT)) {
            int unresolvedReferencesCount = node.get(UNRESOLVED_REFERENCE_COUNT).intValue();
            node.set(UNRESOLVED_REFERENCE_COUNT, IntNode.valueOf(unresolvedReferencesCount + 1));
        } else {
            node.set(UNRESOLVED_REFERENCE_COUNT, IntNode.valueOf(1));
        }
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
