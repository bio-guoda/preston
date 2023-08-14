package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
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

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private LinkedList<ObjectNode> nodes = new LinkedList<>();
    private final TreeMap<String, Map<Long, ObjectNode>> requestedIds;
    public static final String UNRESOLVED_REFERENCE_COUNT = "unresolvedReferenceCount";

    public TaxonWorksJSONStreamHandler(ContentStreamHandler contentStreamHandler,
                                       OutputStream os,
                                       TreeMap<String, Map<Long, ObjectNode>> requestedIds) {
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
                    for (JsonNode node : jsonNode) {
                        handleNodeFor(version, node);
                    }
                    handleNodeFor(version, jsonNode);
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
                resolvedNodes.add(resolved);
            }
        } else if (isOTU(jsonNode)) {
            //resolveId(jsonNode, "biological_association_subject_id");
            //resolveId(jsonNode, "biological_association_object_id");
        } else if (isName(jsonNode)) {
//            ObjectNode objectNode = resolveId(jsonNode, "taxon_name_id");
//            if (objectNode != null
//                    && jsonNode.has("parent_id")
//                    && jsonNode.hasNonNull("parent_id")) {
//                registerIdForNode(jsonNode.get("parent_id").asLong(), objectNode, "taxon_name_id");
//            }
        }
        resolvedNodes.forEach(this::notifyResolution);
    }

    private boolean isName(JsonNode jsonNode) {
        return jsonNode.has("nomenclatural_code");
    }

    private boolean isOTU(JsonNode jsonNode) {
        return jsonNode.has("globi_id")
                && StringUtils.startsWith(
                jsonNode.get("global_id").asText(),
                "gid://taxon-works/Otu/81982"
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
