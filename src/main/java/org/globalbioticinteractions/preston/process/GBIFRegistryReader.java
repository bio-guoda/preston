package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.globalbioticinteractions.preston.model.RefNodeURI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class GBIFRegistryReader extends RefNodeProcessor {
    public static final Map<String, RefNodeType> TYPE_MAP = new HashMap<String, RefNodeType>() {{
        put("DWC_ARCHIVE", RefNodeType.DWCA);
        put("EML", RefNodeType.EML);
    }};
    public static final String GBIF_DATASET_API_ENDPOINT = "https://api.gbif.org/v1/dataset";
    private final Log LOG = LogFactory.getLog(GBIFRegistryReader.class);

    public GBIFRegistryReader(RefNodeListener listener) {
        super(listener);
    }

    private static RefNode nextPage(RefNode previousPage, int offset, int limit, RefNodeEmitter emitter) {
        String uri = GBIF_DATASET_API_ENDPOINT + "?offset=" + offset + "&limit=" + limit;
        RefNode currentPageURI = new RefNodeString(previousPage, RefNodeType.URI, uri);
        emitter.emit(currentPageURI);
        RefNodeURI datasetURI = new RefNodeURI(currentPageURI, RefNodeType.GBIF_DATASETS_JSON, URI.create(uri));
        emitter.emit(datasetURI);
        return datasetURI;
    }

    public static void parse(InputStream resourceAsStream, RefNodeEmitter emitter, RefNode node) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(resourceAsStream);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {
                    String uuid = result.get("key").asText();
                    RefNodeString datasetUUID = new RefNodeString(node, RefNodeType.UUID, uuid);
                    emitter.emit(datasetUUID);
                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            URI url = URI.create(urlString);
                            String type = endpoint.get("type").asText();
                            RefNodeType refNodeType = TYPE_MAP.get(type);
                            if (refNodeType != null) {
                                RefNodeString refNode = new RefNodeString(datasetUUID, RefNodeType.URI, urlString);
                                emitter.emit(refNode);
                                emitter.emit(new RefNodeURI(refNode, refNodeType, url));
                            }
                        }
                    }
                }
            }
        }

        boolean endOfRecords = !jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true);
        if (!endOfRecords && jsonNode.has("offset") && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            int limit = jsonNode.get("limit").asInt();
            nextPage(node, offset + limit, limit, emitter);
        }

    }

    @Override
    public void on(RefNode refNode) {
        if (refNode.equivalentTo(Seeds.SEED_NODE_GBIF)) {
            RefNode refNodeRegistry = new RefNodeString(Seeds.SEED_NODE_GBIF, RefNodeType.URI, GBIF_DATASET_API_ENDPOINT);
            emit(refNodeRegistry);
            emit(new RefNodeURI(refNodeRegistry, RefNodeType.GBIF_DATASETS_JSON, URI.create(GBIF_DATASET_API_ENDPOINT)));
        } else if (refNode.getType() == RefNodeType.GBIF_DATASETS_JSON) {
            try {
                parse(refNode.getData(), this, refNode);
            } catch (IOException e) {
                LOG.warn("failed to handle [" + refNode.getLabel() + "]", e);
            }
        }

    }
}
