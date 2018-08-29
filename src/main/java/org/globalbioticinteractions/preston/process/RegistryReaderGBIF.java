package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.globalbioticinteractions.preston.RefNodeConstants.DATASET_REGISTRY_OF;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_CONTENT;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_PART;

public class RegistryReaderGBIF extends RefNodeProcessor {
    private static final List<String> SUPPORTED_ENDPOINT_TYPES = Arrays.asList("DWC_ARCHIVE","EML");

    private static final String GBIF_DATASET_API_ENDPOINT = "https://api.gbif.org/v1/dataset";
    private final Log LOG = LogFactory.getLog(RegistryReaderGBIF.class);

    public RegistryReaderGBIF(RefNodeListener listener) {
        super(listener);
    }

    @Override
    public void on(RefNodeRelation relation) {
        if (relation.getTarget().equivalentTo(Seeds.SEED_NODE_GBIF)) {
            RefNode refNodeRegistry = new RefNodeString(GBIF_DATASET_API_ENDPOINT);
            emit(new RefNodeRelation(relation.getTarget(), DATASET_REGISTRY_OF, refNodeRegistry));
            emit(new RefNodeRelation(refNodeRegistry, HAS_CONTENT, null));

        } else if (relation.getTarget() != null
                && relation.getSource().getLabel().startsWith(GBIF_DATASET_API_ENDPOINT)
                && relation.getRelationType().equals(RefNodeConstants.HAS_CONTENT)) {
            try {
                parse(relation.getTarget().getData(), this, relation.getTarget());
            } catch (IOException e) {
                LOG.warn("failed to handle [" + relation.getLabel() + "]", e);
            }
        } else if (relation.getTarget() != null) {
            if (StringUtils.startsWith(relation.getTarget().getLabel(), GBIF_DATASET_API_ENDPOINT)) {
                emit(new RefNodeRelation(relation.getTarget(), HAS_CONTENT, null));
            }
        }
    }

    private static void emitNextPage(RefNode previousPage, int offset, int limit, RefNodeEmitter emitter) {
        String uri = GBIF_DATASET_API_ENDPOINT + "?offset=" + offset + "&limit=" + limit;
        RefNode nextPage = new RefNodeString(uri);
        emitter.emit(new RefNodeRelation(previousPage, HAS_PART, nextPage));

    }

    public static void parse(InputStream resourceAsStream, RefNodeEmitter emitter, RefNode dataset) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(resourceAsStream);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {
                    String uuid = result.get("key").asText();
                    RefNodeString datasetUUID = new RefNodeString(uuid);
                    emitter.emit(new RefNodeRelation(dataset, RefNodeConstants.HAS_PART, datasetUUID));

                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            String type = endpoint.get("type").asText();
                            if (SUPPORTED_ENDPOINT_TYPES.contains(type)) {
                                RefNodeString dataArchive = new RefNodeString(urlString);
                                emitter.emit(new RefNodeRelation(datasetUUID, RefNodeConstants.HAS_PART, dataArchive));

                                emitter.emit(new RefNodeRelation(dataArchive, RefNodeConstants.HAS_CONTENT, null));
                            }
                        }
                    }
                }
            }
        }

        boolean endOfRecords = jsonNode == null || (!jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true));
        if (!endOfRecords && jsonNode.has("offset") && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            int limit = jsonNode.get("limit").asInt();
            emitNextPage(dataset, offset + limit, limit, emitter);
        }

    }

}
