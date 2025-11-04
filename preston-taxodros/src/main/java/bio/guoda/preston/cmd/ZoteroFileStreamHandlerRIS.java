package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ZoteroUtil;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZoteroFileStreamHandlerRIS extends ZoteroFileStreamHandlerAbstract {

    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileStreamHandlerRIS.class);
    private final Dereferencer<InputStream> timedDereferencer;
    private final Persisting persisting;

    public ZoteroFileStreamHandlerRIS(ContentStreamHandler contentStreamHandler,
                                      OutputStream os,
                                      Persisting persisting,
                                      Dereferencer<InputStream> deref,
                                      IRI provenanceAnchor) {
        super(contentStreamHandler, os, provenanceAnchor);
        this.persisting = persisting;
        this.timedDereferencer = uri -> {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            try {
                return deref.get(uri);
            } finally {
                stopWatch.stop();
            }
        };

    }


    @Override
    void handleZoteroRecord(JsonNode zoteroRecord, String iriString, AtomicBoolean foundAtLeastOne) throws ContentStreamException, IOException {
        ObjectNode zenodoRecord = new ObjectMapper().createObjectNode();

        String zoteroAttachmentDownloadUrl = ZoteroUtil.getAttachmentDownloadUrl(zoteroRecord);
        String providedContentId = null;

        if (hasPdfAttachment(zoteroRecord, zoteroAttachmentDownloadUrl)) {
            String filename = zoteroRecord.at("/data/filename").asText();
            if (StringUtils.isNoneBlank(filename)) {
                ZenodoMetaUtil.setFilename(zenodoRecord, filename);
            }
            String md5 = zoteroRecord.at("/data/md5").asText();
            if (StringUtils.isNoneBlank(md5)) {
                providedContentId = HashType.md5.getPrefix() + md5;
                ZenodoMetaUtil.appendIdentifier(zenodoRecord, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, providedContentId);
            }
            String zoteroItemUrl = zoteroRecord.at("/links/up/href").asText();
            IRI zoteroItemIRI = RefNodeFactory.toIRI(zoteroItemUrl);

            DerferencerFactory derferencerFactory = () -> timedDereferencer;

            InputStream itemInputStream = ContentQueryUtil.getContent(
                    zoteroItemIRI,
                    derferencerFactory,
                    LOG
            );

            JsonNode itemData = new ObjectMapper().readTree(itemInputStream);
            if (isPrimaryAttachmentOf(zoteroAttachmentDownloadUrl, itemData)) {
                boolean isLikelyZoteroRecord = appendJournalArticleMetaData(
                        iriString,
                        itemData,
                        zenodoRecord
                );

                ZenodoMetaUtil.appendIdentifier(zenodoRecord, ZenodoMetaUtil.IS_COMPILED_BY, RefNodeConstants.PRESTON_DOI, ZenodoMetaUtil.RESOURCE_TYPE_SOFTWARE);

                if (isLikelyZoteroRecord) {
                    foundAtLeastOne.set(true);
                    writeRecord(foundAtLeastOne, zenodoRecord);
                }
            }

        }
    }

    private boolean isPrimaryAttachmentOf(String zoteroAttachmentDownloadUrl, JsonNode itemData) {
        String primaryAttachment = itemData.at("/links/attachment/href").asText();
        return StringUtils.isNotBlank(primaryAttachment) && StringUtils.startsWith(zoteroAttachmentDownloadUrl, primaryAttachment);
    }

    private boolean hasPdfAttachment(JsonNode zoteroRecord, String zoteroAttachmentDownloadUrl) {
        boolean hasPdfAttachment = false;
        if (StringUtils.isNoneBlank(zoteroAttachmentDownloadUrl)) {
            String contentType = zoteroRecord.at("/data/contentType").asText();
            if (StringUtils.equals(contentType, "application/pdf")) {
                hasPdfAttachment = true;
            }
        }
        return hasPdfAttachment;
    }

    private boolean appendJournalArticleMetaData(String iriString, JsonNode jsonNode, ObjectNode objectNode) {
        JsonNode reference = jsonNode.at("/links/self/href");
        JsonNode creators = jsonNode.at("/data/creators");
        boolean isLikelyZoteroRecord = !reference.isMissingNode()
                && !creators.isMissingNode()
                && StringUtils.contains(reference.asText(), "zotero.org");
        if (isLikelyZoteroRecord) {
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_PART_OF, getProvenanceAnchor().getIRIString());
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PUBLICATION);
            ZenodoMetaUtil.setType(objectNode, "application/json");
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, reference.asText());

            List<String> creatorList = ZoteroUtil.parseCreators(creators);
            setArray(objectNode, creatorList, RISUtil.RIS_AUTHOR_NAME);

            List<String> tagValues = ZoteroUtil.parseKeywords(jsonNode);
            setArray(objectNode, tagValues, RISUtil.RIS_KEYWORD);

            String itemType = jsonNode.at("/data/itemType").asText();
            objectNode.put(RISUtil.RIS_PUBLICATION_TYPE,
                    ZoteroUtil.ZOTERO_TO_RIS_PUB_TYPE_TRANSLATION_TABLE.getOrDefault(itemType, "other")
            );
            String dateStringParsed = ZoteroUtil.getPublicationDate(jsonNode);
            objectNode.put(RISUtil.RIS_PUBLICATION_DATE, dateStringParsed);
            objectNode.put(RISUtil.RIS_TITLE, ZoteroUtil.getTitle(jsonNode));


            if (StringUtils.equals(itemType, ZoteroUtil.ZOTERO_JOURNAL_ARTICLE)) {
                objectNode.put(RISUtil.RIS_JOURNAL_TITLE, ZoteroUtil.getJournalTitle(jsonNode));
                objectNode.put(RISUtil.RIS_JOURNAL_VOLUME, ZoteroUtil.getJournalVolume(jsonNode));
                objectNode.put(RISUtil.RIS_JOURNAL_ISSUE, ZoteroUtil.getJournalIssue(jsonNode));
                objectNode.put(RISUtil.RIS_JOURNAL_PAGES, ZoteroUtil.getJournalPages(jsonNode));
            }

            if (Arrays.asList(ZoteroUtil.ZOTERO_BOOK, ZoteroUtil.ZOTERO_BOOK_SECTION).contains(itemType)) {
                objectNode.put(RISUtil.RIS_IMPRINT_PUBLISHER, ZoteroUtil.getPublisherName(jsonNode));
                objectNode.put(RISUtil.RIS_TITLE, ZoteroUtil.getBookTitle(jsonNode));
                objectNode.put(RISUtil.RIS_JOURNAL_PAGES, ZoteroUtil.getJournalVolume(jsonNode));
            }

            String doi = ZoteroUtil.getDOI(jsonNode);
            if (StringUtils.isNotBlank(doi)) {
                objectNode.put(RISUtil.RIS_DOI, doi);
            }

            String abstractNote = ZoteroUtil.getAbstract(jsonNode);
            if (StringUtils.isNotBlank(abstractNote)) {
                objectNode.put(RISUtil.RIS_ABSTRACT, abstractNote);
            }
        }
        return isLikelyZoteroRecord;
    }

    private static void setArray(ObjectNode objectNode, List<String> tagValues, String risKeyword) {
        ArrayNode keywords = new ObjectMapper().createArrayNode();
        tagValues.forEach(keywords::add);
        objectNode.set(risKeyword, keywords);
    }


    private void appendAttachmentInfo(ObjectNode objectNode, String zoteroAttachmentDownloadUrl, String zoteroItemUrl) throws ContentStreamException {
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, getZoteroLSID(zoteroItemUrl));
        appendContentId(objectNode, zoteroAttachmentDownloadUrl, HashType.md5);
        appendContentId(objectNode, zoteroAttachmentDownloadUrl, HashType.sha256);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, getZoteroSelector(zoteroItemUrl));
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, getZoteroHtmlPage(zoteroItemUrl));
    }

    private void appendContentId(ObjectNode objectNode, String zoteroAttachmentDownloadUrl, HashType hashType) throws ContentStreamException {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            StreamHandlerUtil.appendContentId(
                    objectNode,
                    zoteroAttachmentDownloadUrl,
                    hashType,
                    this.timedDereferencer,
                    this.persisting
            );
        } finally {
            stopWatch.stop();
            ContentQueryUtil.logReponseTime(stopWatch, zoteroAttachmentDownloadUrl, "contentid calculated in ", LOG);
        }

    }

    private String getZoteroSelector(String href) {
        return "zotero://select" + URI.create(href).getPath();
    }

    private String getZoteroHtmlPage(String href) {
        return "https://zotero.org" + URI.create(href).getPath();
    }

    private String getZoteroLSID(String href) {
        return "urn:lsid:zotero.org" + StringUtils.replace(URI.create(href).getPath(), "/", ":");
    }


    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        if (foundAtLeastOne.get()) {
            RISUtil.writeAsRIS(objectNode, getOutputStream());
        }
    }

}
