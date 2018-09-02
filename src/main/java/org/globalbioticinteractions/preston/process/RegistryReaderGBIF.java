package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.MimeTypes;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.store.Predicate;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.globalbioticinteractions.preston.RefNodeConstants.CONTINUATION_OF;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAD_MEMBER;
import static org.globalbioticinteractions.preston.RefNodeConstants.SEED_OF;

public class RegistryReaderGBIF extends ProcessorReadOnly {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("DWC_ARCHIVE", MimeTypes.MIME_TYPE_DWCA);
        put("EML", MimeTypes.MIME_TYPE_EML);
    }};

    private static final String GBIF_DATASET_API_ENDPOINT = "https://api.gbif.org/v1/dataset";
    private final Log LOG = LogFactory.getLog(RegistryReaderGBIF.class);

    public RegistryReaderGBIF(BlobStoreReadOnly blobStoreReadOnly, RefStatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Triple statement) {
        if (Seeds.SEED_NODE_GBIF.equals(statement.getSubject())
                && SEED_OF.equals(statement.getPredicate())) {
            IRI refNodeRegistry = RefNodeFactory.toIRI(GBIF_DATASET_API_ENDPOINT);
            emitPageRequest(this, refNodeRegistry);
        } else if (statement.getSubject() instanceof IRI
                && statement.getObject() instanceof IRI
                && statement.getObject().toString().startsWith("<" + GBIF_DATASET_API_ENDPOINT)
                && RefNodeFactory.isDerivedFrom(statement)) {
            try {
                parse((IRI) statement.getSubject(), this, (IRI)statement.getObject(), get((IRI) statement.getSubject()));
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
        }
    }

    private static void emitNextPage(IRI previousPage, int offset, int limit, RefStatementEmitter emitter) {
        String uri = GBIF_DATASET_API_ENDPOINT + "?offset=" + offset + "&limit=" + limit;
        IRI nextPage = RefNodeFactory.toIRI(uri);
        emitter.emit(RefNodeFactory.toStatement(nextPage, CONTINUATION_OF, previousPage));
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(RefStatementEmitter emitter, IRI nextPage) {
        emitter.emit(RefNodeFactory.toStatement(nextPage, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toContentType(MimeTypes.MIME_TYPE_JSON)));
        emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), Predicate.WAS_DERIVED_FROM, nextPage));
    }

    public static void parse(IRI currentPageContent, RefStatementEmitter emitter, IRI currentPage, InputStream in) throws IOException {
        emitter.emit(RefNodeFactory.toStatement(Seeds.SEED_NODE_GBIF, HAD_MEMBER, currentPageContent));
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {
                    String uuid = result.get("key").asText();
                    IRI datasetUUID = RefNodeFactory.toUUID(uuid);
                    emitter.emit(RefNodeFactory.toStatement(currentPageContent, RefNodeConstants.HAD_MEMBER, datasetUUID));

                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            String type = endpoint.get("type").asText();

                            if (SUPPORTED_ENDPOINT_TYPES.containsKey(type)) {
                                IRI dataArchive = RefNodeFactory.toIRI(urlString);
                                emitter.emit(RefNodeFactory.toStatement(datasetUUID, RefNodeConstants.HAD_MEMBER, dataArchive));
                                emitter.emit(RefNodeFactory.toStatement(dataArchive, RefNodeConstants.HAS_FORMAT, RefNodeFactory.toContentType(SUPPORTED_ENDPOINT_TYPES.get(type))));
                                emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toBlank(), Predicate.WAS_DERIVED_FROM, dataArchive));
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
            emitNextPage(currentPage, offset + limit, limit, emitter);
        }

    }

}
