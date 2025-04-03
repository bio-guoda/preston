package bio.guoda.preston.zenodo;

import bio.guoda.preston.DateUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.cmd.ZenodoMetaUtil;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.VersionUtil;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
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

import static bio.guoda.preston.cmd.ZenodoMetaUtil.HAS_VERSION;
import static bio.guoda.preston.cmd.ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER;
import static bio.guoda.preston.cmd.ZenodoMetaUtil.IS_DERIVED_FROM;
import static bio.guoda.preston.zenodo.ZenodoUtils.delete;
import static bio.guoda.preston.zenodo.ZenodoUtils.getObjectMapper;

public class ZenodoMetadataFileStreamHandler implements ContentStreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ZenodoMetadataFileStreamHandler.class);


    private final Dereferencer<InputStream> dereferencer;
    private final ZenodoConfig ctx;
    private final StatementEmitter emitter;
    private final Collection<Quad> candidateFileAttachments;
    private ContentStreamHandler contentStreamHandler;

    public ZenodoMetadataFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                           Dereferencer<InputStream> inputStreamDereferencer,
                                           StatementEmitter emitter,
                                           ZenodoConfig ctx,
                                           Collection<Quad> candidateFileAttachments) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.emitter = emitter;
        this.ctx = ctx;
        this.candidateFileAttachments = new ArrayList<>(candidateFileAttachments);
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
                    JsonNode zenodoMetadata;
                    try {
                        zenodoMetadata = getObjectMapper().readTree(line);
                    } catch (JsonProcessingException ex) {
                        // likely not a json-lines file, so skip altogether
                        break;
                    }
                    try {
                        handleJson(coordinate, zenodoMetadata);
                    } catch (IOException ex) {
                        LOG.warn("failed to handle [" + coordinate + "] as jsonlines", ex);
                    }
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("failed to handle [" + iriString + "]", e);
        }

        return foundAtLeastOne.get();
    }

    private void handleJson(IRI coordinate, JsonNode zenodoMetadata) throws IOException {
        String resourceType = "";
        if (maybeContainsPrestonEnabledZenodoMetadata(zenodoMetadata)) {
            List<String> recordIds = new ArrayList<>();
            List<String> contentIds = new ArrayList<>();
            List<String> origins = new ArrayList<>();
            origins.add(coordinate.getIRIString());

            collectIds(zenodoMetadata, recordIds, contentIds, origins);
            if (recordIds.isEmpty()) {
                throw new IOException("cannot publish zenodo record: no lsid found in [" + coordinate.getIRIString() + "]");
            }

            resourceType = updateResourceType(resourceType, zenodoMetadata);

            Dereferencer<InputStream> adHocQueryDereferencer = ResourcesHTTP::asInputStream;
            Collection<Pair<Long, String>> foundDeposits = ZenodoUtils.findRecordsByAlternateIds(
                    ctx,
                    recordIds,
                    resourceType,
                    adHocQueryDereferencer
            );
            createNewVersion(coordinate, zenodoMetadata, recordIds, contentIds, origins, foundDeposits);

        }
    }

    static String updateResourceType(String resourceType, JsonNode zenodoMetadata) {
        JsonNode uploadType = zenodoMetadata.at("/metadata/upload_type");
        if (!uploadType.isMissingNode()) {
            resourceType = uploadType.asText();
        }
        JsonNode imageType = zenodoMetadata.at("/metadata/image_type");
        if (!imageType.isMissingNode()) {
            resourceType = resourceType + "-" + imageType.asText();
        }
        return resourceType;
    }

    private void collectIds(JsonNode zenodoMetadata, List<String> recordIds, List<String> contentIds, List<String> origins) {
        JsonNode alternateIdentifiers = zenodoMetadata.at("/metadata/related_identifiers");
        if (alternateIdentifiers != null && alternateIdentifiers.isArray()) {
            for (JsonNode alternateIdentifier : alternateIdentifiers) {
                JsonNode relation = alternateIdentifier.at("/relation");
                JsonNode identifier = alternateIdentifier.at("/identifier");
                if (relation != null && identifier != null) {
                    if (StringUtils.equals(relation.asText(), IS_ALTERNATE_IDENTIFIER)) {
                        String identiferText = identifier.asText();
                        if (StringUtils.startsWith(identiferText, "urn:lsid")) {
                            recordIds.add(identiferText);
                        } else if (StringUtils.startsWith(identiferText, "hash:")) {
                            contentIds.add(identiferText);
                        }
                    } else if (StringUtils.equals(relation.asText(), IS_DERIVED_FROM)) {
                        String identiferText = identifier.asText();
                        if (StringUtils.isNotBlank(identiferText)) {
                            origins.add(identiferText);
                        }
                    } else if (StringUtils.equals(relation.asText(), HAS_VERSION)) {
                        String identiferText = identifier.asText();
                        if (StringUtils.startsWith(identiferText, "hash:")) {
                            contentIds.add(identiferText);
                        }
                    }
                }
            }
        }
    }

    private void createNewVersion(IRI coordinate,
                                  JsonNode zenodoMetadata,
                                  List<String> recordIds,
                                  List<String> contentIds,
                                  List<String> origins,
                                  Collection<Pair<Long, String>> foundDeposits) throws IOException {
        List<Long> existingIds = foundDeposits
                .stream()
                .filter(x -> !StringUtils.equals(x.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .distinct()
                .collect(Collectors.toList());

        ZenodoContext ctxLocal = new ZenodoContext(this.ctx);

        ZenodoUtils.credentialsOrThrow(ctxLocal);


        if (!hasAllowedPublicationDate(zenodoMetadata, ctx)) {
            throw new IOException("missing publication date for [" + zenodoMetadata.toPrettyString() + "]");
        }

        if (ctx.shouldPublishRestrictedOnly()) {
            JsonNode at = zenodoMetadata.at("/metadata");
            if (at.isObject()) {
                ZenodoMetaUtil.setRestricted((ObjectNode) at);
            }
        }

        try {
            if (existingIds.size() == 0 && !ctx.shouldUpdateMetadataOnly()) {
                ctxLocal = ZenodoUtils.createEmptyDeposit(ctxLocal);
                updateMetadata(ctxLocal, zenodoMetadata);
                uploadContentAndPublish(zenodoMetadata, contentIds, ctxLocal);
                emitPublicationStatements(recordIds, contentIds, origins, ctxLocal);
            } else if (existingIds.size() == 1 && ctx.createNewVersionForExisting()) {
                ctxLocal.setDepositId(existingIds.get(0));
                ctxLocal = ZenodoUtils.createNewVersion(ctxLocal);
                deleteExistingContentIfPresent(ctxLocal);
                updateMetadata(ctxLocal, zenodoMetadata);
                uploadContentAndPublish(zenodoMetadata, contentIds, ctxLocal);
                emitPublicationStatements(recordIds, contentIds, origins, ctxLocal);
            } else if (existingIds.size() == 1 && ctx.shouldUpdateMetadataOnly()) {
                ctxLocal.setDepositId(existingIds.get(0));
                ctxLocal = ZenodoUtils.editExistingVersion(ctxLocal);
                updateMetadata(ctxLocal, zenodoMetadata);
                ZenodoUtils.publish(ctxLocal);
                emitPublicationStatements(recordIds, contentIds, origins, ctxLocal);
            } else {
                emitRelatedExistingRecords(coordinate, existingIds, ctxLocal);
            }
        } catch (Throwable e) {
            LOG.warn("unexpected error while handling [" + coordinate.getIRIString() + "]", e);
            attemptCleanupAndRethrow(ctxLocal, e);
        } finally {
            candidateFileAttachments.clear();
        }
    }

    private void emitPublicationStatements(List<String> recordIds, List<String> contentIds, List<String> origins, ZenodoContext ctxLocal) {
        emitRelations(recordIds, contentIds, origins, ctxLocal);

        emitRelations(ctxLocal,
                RefNodeConstants.HAD_MEMBER,
                candidateFileAttachments.stream()
                        .map(Quad::getObject)
                        .filter(q -> q instanceof IRI).map(q -> ((IRI) q).getIRIString())
                        .collect(Collectors.toList()));

        emitRelations(ctxLocal,
                RefNodeConstants.HAD_MEMBER,
                candidateFileAttachments.stream()
                        .map(q -> fileIRI(ctxLocal, q))
                        .map(IRI::getIRIString)
                        .collect(Collectors.toList()));

        candidateFileAttachments
                .stream()
                .map(q -> RefNodeFactory.toStatement(fileIRI(ctxLocal, q), q.getPredicate(), q.getObject()))
                .forEach(emitter::emit);

        emitRefreshed(ctxLocal);
    }

    private IRI fileIRI(ZenodoContext ctxLocal, Quad q) {
        return RefNodeFactory.toIRI(getRecordUrl(ctxLocal).getIRIString() + "/files/" + filenameFor(q));
    }

    private void updateMetadata(ZenodoContext ctxLocal, JsonNode zenodoMetadata) throws IOException {
        String input = getObjectMapper().writer().writeValueAsString(zenodoMetadata);
        ZenodoUtils.update(ctxLocal, StringUtils.replace(input, "{{ ZENODO_DEPOSIT_ID }}", Long.toString(ctxLocal.getDepositId())));
    }

    private void deleteExistingContentIfPresent(ZenodoContext ctxLocal) throws IOException {
        List<IRI> fileEndpoints = ZenodoUtils.getFileEndpoints(ctxLocal);
        for (IRI fileEndpoint : fileEndpoints) {
            try (InputStream inputStream = delete(fileEndpoint, ctxLocal)) {
                IOUtils.copy(inputStream, NullOutputStream.INSTANCE);
            } catch (IOException e) {
                throw new IOException("failed to delete existing file [" + fileEndpoint.getIRIString() + "] for deposition [" + ctxLocal.getMetadata() + "]", e);
            }
        }
    }

    public static boolean hasAllowedPublicationDate(JsonNode zenodoMetadata, ZenodoConfig ctx) {
        return !zenodoMetadata.at("/metadata/publication_date").isMissingNode()
                || ctx.shouldAllowEmptyPublicationDate();
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
            if (ctxLocal.getDepositId() != null) {
                ZenodoUtils.delete(ctxLocal);
            }
        } catch (IOException ex) {
            // ignore
        }
        throw new IOException(e);
    }

    private void emitRelations(ZenodoContext ctxLocal, IRI relationType, List<String> orgins) {
        orgins
                .stream()
                .map(RefNodeFactory::toIRI)
                .forEach(o -> emitRecordRelation(ctxLocal, relationType, o));
    }

    private void emitRecordRelation(ZenodoContext subject, IRI verb, IRI object) {
        emitter.emit(RefNodeFactory.toStatement(
                getRecordUrl(subject),
                verb,
                object)
        );
    }

    private IRI getRecordUrl(ZenodoContext ctxLocal) {
        return getRecordUrl(ctxLocal, ctxLocal.getDepositId());
    }

    private void uploadContentAndPublish(JsonNode zenodoMetadata, List<String> ids, ZenodoContext ctx) throws IOException {
        if (hasCustomFilename(zenodoMetadata)) {
            uploadAttemptSingleFile(zenodoMetadata, ids, ctx);
        } else if (candidateFileAttachments.size() > CmdZenodo.MAX_ZENODO_FILE_ATTACHMENTS) {
            throw new IOException("cannot publish more than " + CmdZenodo.MAX_ZENODO_FILE_ATTACHMENTS + " for a Zenodo deposit but found [" + candidateFileAttachments.size() + "]: to many file attachments specified for [" + zenodoMetadata.toPrettyString() + "]");
        } else if (candidateFileAttachments.size() > 0) {
            uploadAttemptOneOrMoreFiles(ctx);
        } else {
            throw new IOException("cannot publish: no file attachments specified for [" + zenodoMetadata.toPrettyString() + "]");
        }
        ZenodoUtils.publish(ctx);
    }

    private void uploadAttemptOneOrMoreFiles(ZenodoContext ctx) throws IOException {
        ArrayList<Quad> candidates = new ArrayList<>(this.candidateFileAttachments);
        for (Quad versionStatement : candidates) {
            String filename = filenameFor(versionStatement);
            IRI version = VersionUtil.mostRecentVersion(versionStatement);
            if (version != null && StringUtils.isNoneBlank(filename)) {
                String msg = "upload [" + version + "] as [" + filename + "]";
                LOG.info(msg + " started...");
                ZenodoUtils.upload(ctx,
                        filename,
                        new DereferencingEntity(dereferencer, version)
                );
                LOG.info(msg + " finished.");
            }
        }
    }

    static String filenameFor(Quad quad) {
        String filename = null;
        RDFTerm nameInspiration = quad.getSubject() instanceof IRI ? quad.getSubject() : quad.getObject();
        if (nameInspiration instanceof IRI) {
            IRI versionAlias = (IRI) nameInspiration;
            String[] split = StringUtils.split(versionAlias.getIRIString(), "/");
            filename = split[split.length - 1];
        }
        return filename;
    }

    private void uploadAttemptSingleFile(JsonNode metadata, List<String> ids, ZenodoContext ctx) throws IOException {
        JsonNode filename = getFilenameNode(metadata);


        List<IRI> contentIdCandidate = ids.stream()
                .map(RefNodeFactory::toIRI)
                .filter(HashKeyUtil::isValidHashKey)
                .collect(Collectors.toList());

        if (contentIdCandidate.size() == 0) {
            LOG.info("no content id found for [" + filename.asText() + "] in candidate ids [" + StringUtils.join(ids) + "] for [" + metadata.toPrettyString() + "]");
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

    private boolean hasCustomFilename(JsonNode metadata) {
        JsonNode filename = metadata.at("/metadata/filename");
        boolean singleFilename = false;
        if (!filename.isMissingNode()) {
            singleFilename = true;
        }
        return singleFilename;
    }

    private JsonNode getFilenameNode(JsonNode metadata) throws IOException {
        JsonNode filename = metadata.at("/metadata/filename");
        boolean singleFilename = false;
        if (!filename.isMissingNode()) {
            singleFilename = true;
        }

        if (!singleFilename) {
            throw new IOException("no filename specified for [" + metadata.toString() + "]");
        }
        return filename;
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
