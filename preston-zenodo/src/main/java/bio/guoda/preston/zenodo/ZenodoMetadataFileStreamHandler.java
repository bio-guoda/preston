package bio.guoda.preston.zenodo;

import bio.guoda.preston.DateUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.ZenodoMetaUtil;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static bio.guoda.preston.zenodo.ZenodoUtils.getObjectMapper;

public class ZenodoMetadataFileStreamHandler implements ContentStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ZenodoMetadataFileStreamHandler.class);


    private final Dereferencer<InputStream> dereferencer;
    private final ZenodoConfig ctx;
    private final StatementEmitter emitter;
    private ContentStreamHandler contentStreamHandler;

    public ZenodoMetadataFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                           Dereferencer<InputStream> inputStreamDereferencer,
                                           StatementEmitter emitter,
                                           ZenodoConfig ctx) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.emitter = emitter;
        this.ctx = ctx;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

            for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                IRI coordinate = RefNodeFactory.toIRI("line:" + iriString + "!/L" + lineNumber);
                String line = reader.readLine();
                if (line == null) {
                    break;
                } else {
                    try {
                        attemptToHandleJSON(line, coordinate);
                    } catch (IOException ex) {
                        LOG.warn("failed to handle [" + line + "] in [" + coordinate + "]", ex);
                    }
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("failed to handle [" + iriString + "]", e);
        }

        return foundAtLeastOne.get();
    }

    private void attemptToHandleJSON(String line, IRI coordinate) throws IOException {
        JsonNode zenodoMetadata = getObjectMapper().readTree(line);
        if (maybeContainsPrestonEnabledZenodoMetadata(zenodoMetadata)) {
            List<String> recordIds = new ArrayList<>();
            List<String> contentIds = new ArrayList<>();
            List<String> origins = new ArrayList<>();
            origins.add(coordinate.getIRIString());
            JsonNode alternateIdentifiers = zenodoMetadata.at("/metadata/related_identifiers");
            if (alternateIdentifiers != null && alternateIdentifiers.isArray()) {
                for (JsonNode alternateIdentifier : alternateIdentifiers) {
                    JsonNode relation = alternateIdentifier.at("/relation");
                    JsonNode identifier = alternateIdentifier.at("/identifier");
                    if (relation != null && identifier != null) {
                        if (StringUtils.equals(relation.asText(), "isAlternateIdentifier")) {
                            String identiferText = identifier.asText();
                            if (StringUtils.startsWith(identiferText, "urn:lsid")) {
                                recordIds.add(identiferText);
                            } else if (StringUtils.startsWith(identiferText, "hash:")) {
                                contentIds.add(identiferText);
                            }
                        } else if (StringUtils.equals(relation.asText(), "isDerivedFrom")) {
                            String identiferText = identifier.asText();
                            if (StringUtils.isNotBlank(identiferText)) {
                                origins.add(identiferText);
                            }
                        } else if (StringUtils.equals(relation.asText(), "hasVersion")) {
                            String identiferText = identifier.asText();
                            if (StringUtils.startsWith(identiferText, "hash:")) {
                                contentIds.add(identiferText);
                            }
                        }
                    }
                }
            }
            if (recordIds.isEmpty()) {
                throw new IOException("cannot publish zenodo record: no lsid found in [" + coordinate.getIRIString() + "]");
            }

            Collection<Pair<Long, String>> foundDeposits = ZenodoUtils.findByAlternateIds(ctx, recordIds);
            deleteDraftsIfPresent(foundDeposits, recordIds);
            createNewVersion(coordinate, zenodoMetadata, recordIds, contentIds, origins, foundDeposits);

        }

    }

    private void deleteDraftsIfPresent(Collection<Pair<Long, String>> foundDeposits, List<String> recordIds) throws IOException {
        List<Long> draftIds = foundDeposits
                .stream()
                .filter(x -> StringUtils.equals(x.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .distinct()
                .collect(Collectors.toList());

        ZenodoContext ctxLocal = new ZenodoContext(this.ctx);
        for (Long draftId : draftIds) {
            ctxLocal.setDepositId(draftId);
            try {
                String msg = "deleting draft deposit [" + ctxLocal.getDepositId() + "] associated with [{" + StringUtils.join(recordIds, "\"}, {\"") + "}]";
                LOG.info(msg + " ...");
                ZenodoUtils.delete(ctxLocal);
                LOG.info(msg + " done.");
            } catch (IOException ex) {
                throw new IOException("failed to delete draft/unsubmitted deposit id [" + draftId + "]");
            }

        }
    }

    private void createNewVersion(IRI coordinate, JsonNode zenodoMetadata, List<String> recordIds, List<String> contentIds, List<String> origins, Collection<Pair<Long, String>> foundDeposits) throws IOException {
        List<Long> existingIds = foundDeposits
                .stream()
                .filter(x -> !StringUtils.equals(x.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .distinct()
                .collect(Collectors.toList());

        ZenodoContext ctxLocal = new ZenodoContext(this.ctx);

        if (ctx.shouldPublishRestrictedOnly()) {
            JsonNode at = zenodoMetadata.at("/metadata");
            if (at.isObject()) {
                ZenodoMetaUtil.setRestricted((ObjectNode) at);
            }
        }

        try {
            if (existingIds.size() == 0 && !ctx.shouldUpdateMetadataOnly()) {
                ctxLocal = ZenodoUtils.create(ctxLocal, zenodoMetadata);
                uploadContentAndPublish(zenodoMetadata, contentIds, ctxLocal);
                emitRelations(recordIds, contentIds, origins, ctxLocal);
                emitRefreshed(ctxLocal);
            } else if (existingIds.size() == 1 && ctx.createNewVersionForExisting()) {
                ctxLocal.setDepositId(existingIds.get(0));
                ctxLocal = ZenodoUtils.createNewVersion(ctxLocal);
                String input = getObjectMapper().writer().writeValueAsString(zenodoMetadata);
                ZenodoUtils.update(ctxLocal, input);
                uploadContentAndPublish(zenodoMetadata, contentIds, ctxLocal);
                emitRelations(recordIds, contentIds, origins, ctxLocal);
                emitRefreshed(ctxLocal);
            } else if (existingIds.size() == 1 && ctx.shouldUpdateMetadataOnly()) {
                ctxLocal.setDepositId(existingIds.get(0));
                ctxLocal = ZenodoUtils.editExistingVersion(ctxLocal);
                String input = getObjectMapper().writer().writeValueAsString(zenodoMetadata);
                ZenodoUtils.update(ctxLocal, input);
                ZenodoUtils.publish(ctxLocal);
                emitRelations(recordIds, contentIds, origins, ctxLocal);
                emitRefreshed(ctxLocal);
            } else {
                emitRelatedExistingRecords(coordinate, existingIds, ctxLocal);
            }
        } catch (Throwable e) {
            LOG.warn("unexpected error while handling [" + coordinate.getIRIString() + "]", e);
            attemptCleanupAndRethrow(ctxLocal, e);
        }
    }

    private void emitRefreshed(ZenodoContext ctxLocal) {
        emitter.emit(
                RefNodeFactory.toStatement(
                        getRecordUrl(ctxLocal),
                        RefNodeConstants.LAST_REFRESHED_ON,
                        RefNodeFactory.toDateTime(DateUtil.now())
                )
        );
    }

    private void emitRelations(List<String> lsids, List<String> contentIds, List<String> origins, ZenodoContext ctxLocal) {
        emitRelations(ctxLocal, RefNodeConstants.WAS_DERIVED_FROM, origins);
        emitRelations(ctxLocal, RefNodeConstants.ALTERNATE_OF, contentIds);
        emitRelations(ctxLocal, RefNodeConstants.ALTERNATE_OF, lsids);
    }

    private void emitRelatedExistingRecords(IRI coordinate, List<Long> existingIds, ZenodoContext ctxLocal) {
        for (Long existingId : existingIds) {
            emitter.emit(
                    RefNodeFactory.toStatement(
                            getRecordUrl(ctxLocal, existingId),
                            RefNodeConstants.LAST_ACCESSED_ON,
                            RefNodeFactory.toDateTime(DateUtil.now()))
            );
            emitter.emit(
                    RefNodeFactory.toStatement(
                            getRecordUrl(ctxLocal, existingId),
                            RefNodeConstants.SEE_ALSO,
                            coordinate)
            );
        }
    }

    private IRI getRecordUrl(ZenodoContext ctxLocal, Long existingId) {
        return RefNodeFactory.toIRI(ctxLocal.getEndpoint() + "/records/" + existingId);
    }

    private void attemptCleanupAndRethrow(ZenodoContext ctxLocal, Throwable e) throws IOException {
        try {
            ZenodoUtils.delete(ctxLocal);
        } catch (IOException ex) {
            // ignore
        }
        throw new IOException(e);
    }

    private void emitRelations(ZenodoContext ctxLocal, IRI relationType, List<String> orgins) {
        orgins
                .stream()
                .map(RefNodeFactory::toIRI)
                .forEach(o -> emitOrigin(o, ctxLocal, relationType));
    }

    private void emitOrigin(IRI origin, ZenodoContext ctxLocal, IRI wasDerivedFrom) {
        emitter.emit(RefNodeFactory.toStatement(
                getRecordUrl(ctxLocal),
                wasDerivedFrom,
                origin)
        );
    }

    private IRI getRecordUrl(ZenodoContext ctxLocal) {
        return getRecordUrl(ctxLocal, ctxLocal.getDepositId());
    }

    private void uploadContentAndPublish(JsonNode zenodoMetadata, List<String> ids, ZenodoContext ctx) throws IOException {
        uploadAttempt(zenodoMetadata, ids, ctx);
        ZenodoUtils.publish(ctx);
    }

    private void uploadAttempt(JsonNode metadata, List<String> ids, ZenodoContext ctx) throws IOException {
        JsonNode filename = metadata.at("/metadata/filename");
        if (filename.isMissingNode()) {
            throw new IOException("no filename specified for [" + metadata.toString() + "]");
        }

        List<IRI> contentIdCandidate = ids.stream()
                .map(RefNodeFactory::toIRI)
                .filter(HashKeyUtil::isValidHashKey)
                .collect(Collectors.toList());

        if (contentIdCandidate.size() == 0) {
            LOG.info("no content id found for [" + filename + "] in candidate ids [" + StringUtils.join(ids) + "] for [" + metadata.toPrettyString() + "]");
        }

        IOException lastException = null;
        for (IRI iri : contentIdCandidate) {
            String msg = "upload [" + iri + "] as [" + filename.asText() + "]";
            try {
                LOG.info(msg + " started...");
                ZenodoUtils.upload(ctx,
                        filename.asText(),
                        new DereferencingEntity(dereferencer, iri));
                LOG.info(msg + " finished.");
                lastException = null;
                break;
            } catch (IOException e) {
                LOG.info(msg + " failed.");
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    private boolean maybeContainsPrestonEnabledZenodoMetadata(JsonNode jsonNode) {
        return jsonNode.at("/metadata/upload_type") != null
                && jsonNode.at("/metadata/title") != null
                && jsonNode.at("/metadata/related_identifiers") != null;
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
