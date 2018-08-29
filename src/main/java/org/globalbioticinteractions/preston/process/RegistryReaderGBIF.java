package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.globalbioticinteractions.preston.RefNodeConstants.DATASET_REGISTRY_OF;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_CONTENT;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_PART;

public class RegistryReaderGBIF extends RefNodeProcessor {
    public static final Map<String, RefNodeType> TYPE_MAP = new HashMap<String, RefNodeType>() {{
        put("DWC_ARCHIVE", RefNodeType.DWCA);
        put("EML", RefNodeType.EML);
    }};
    public static final String GBIF_DATASET_API_ENDPOINT = "https://api.gbif.org/v1/dataset";
    private final Log LOG = LogFactory.getLog(RegistryReaderGBIF.class);

    public RegistryReaderGBIF(RefNodeListener listener) {
        super(listener);
    }

    @Override
    public void on(RefNodeRelation refNode) {
        if (refNode.equivalentTo(Seeds.SEED_NODE_GBIF)) {
            RefNode refNodeRegistry = new RefNodeString(RefNodeType.URI, GBIF_DATASET_API_ENDPOINT);
            emit(new RefNodeRelation(refNode, DATASET_REGISTRY_OF, refNodeRegistry));
            emit(new RefNodeRelation(refNodeRegistry, HAS_CONTENT, null));

        } else if (refNode.getType() == RefNodeType.GBIF_DATASETS_JSON) {
            try {
                parse(refNode.getData(), this, refNode);
            } catch (IOException e) {
                LOG.warn("failed to handle [" + refNode.getLabel() + "]", e);
            }
        } else if (refNode.getType() == RefNodeType.URI) {
            try {
                String dataString = getDataString(refNode);
                if (StringUtils.startsWith(dataString, GBIF_DATASET_API_ENDPOINT)) {
                    emit(new RefNodeRelation(refNode, HAS_CONTENT, null));
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + refNode.getLabel() + "]", e);
            }

        }
    }

    private String getDataString(RefNode refNode) throws IOException {
        return IOUtils.toString(refNode.getData(), StandardCharsets.UTF_8);
    }

    private static void emitNextPage(RefNode previousPage, int offset, int limit, RefNodeEmitter emitter) {
        String uri = GBIF_DATASET_API_ENDPOINT + "?offset=" + offset + "&limit=" + limit;
        RefNode nextPage = new RefNodeString(RefNodeType.URI, uri);
        emitter.emit(new RefNodeRelation(previousPage, HAS_PART, nextPage));

    }

    public static void parse(InputStream resourceAsStream, RefNodeEmitter emitter, RefNode dataset) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(resourceAsStream);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {
                    String uuid = result.get("key").asText();
                    RefNodeString datasetUUID = new RefNodeString(RefNodeType.UUID, uuid);
                    emitter.emit(new RefNodeRelation(dataset, RefNodeConstants.HAS_PART, datasetUUID));

                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            URI url = URI.create(urlString);
                            String type = endpoint.get("type").asText();
                            RefNodeType refNodeType = TYPE_MAP.get(type);
                            if (refNodeType != null) {
                                RefNodeString dataArchive = new RefNodeString(RefNodeType.URI, urlString);
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
