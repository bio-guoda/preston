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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ZoteroFileStreamHandlerZenodo extends ZoteroFileStreamHandlerAbstract {

    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileStreamHandlerZenodo.class);
    private final Dereferencer<InputStream> timedDereferencer;
    private final List<String> communities;
    private final Persisting persisting;
    private final boolean appendDoiToTitle;

    public ZoteroFileStreamHandlerZenodo(ContentStreamHandler contentStreamHandler,
                                         OutputStream os,
                                         Persisting persisting,
                                         Dereferencer<InputStream> deref,
                                         List<String> communities,
                                         IRI provenanceAnchor,
                                         boolean appendDoiToTitle) {
        super(contentStreamHandler, os, provenanceAnchor);
        this.persisting = persisting;
        this.communities = communities;
        this.timedDereferencer = uri -> {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            try {
                return deref.get(uri);
            } finally {
                stopWatch.stop();
            }
        };
        this.appendDoiToTitle = appendDoiToTitle;

    }


    @Override
    void handleZoteroRecord(JsonNode zoteroRecord, String iriString, AtomicBoolean foundAtLeastOne) throws ContentStreamException, IOException {
        ObjectNode zenodoRecord = new ObjectMapper().createObjectNode();

        String zoteroAttachmentDownloadUrl = ZoteroUtil.getAttachmentDownloadUrl(zoteroRecord);
        String providedContentId = null;

        if (hasPdfAttachment(zoteroRecord)) {
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
            appendAttachmentInfo(
                    zenodoRecord,
                    StringUtils.isBlank(providedContentId) ? zoteroAttachmentDownloadUrl : providedContentId,
                    zoteroItemUrl
            );

            IRI zoteroItemIRI = RefNodeFactory.toIRI(zoteroItemUrl);

            DerferencerFactory derferencerFactory = () -> timedDereferencer;

            InputStream itemInputStream = ContentQueryUtil.getContent(
                    zoteroItemIRI,
                    derferencerFactory,
                    LOG
            );

            JsonNode itemData = new ObjectMapper().readTree(itemInputStream);
            if (StringUtils.isBlank(zoteroAttachmentDownloadUrl)
                    || isPrimaryAttachmentOf(zoteroAttachmentDownloadUrl, itemData)) {
                boolean isLikelyZoteroRecord = appendJournalArticleMetaData(
                        iriString,
                        itemData,
                        zenodoRecord
                );
                if (appendDoiToTitle) {
                    appendDoiToTitle(zenodoRecord, itemData);
                }

                ZenodoMetaUtil.appendIdentifier(zenodoRecord,
                        ZenodoMetaUtil.IS_COMPILED_BY,
                        RefNodeConstants.PRESTON_DOI,
                        ZenodoMetaUtil.RESOURCE_TYPE_SOFTWARE
                );

                if (isLikelyZoteroRecord) {
                    foundAtLeastOne.set(true);
                    writeRecord(foundAtLeastOne, zenodoRecord);
                }
            }

        }
    }

    private void appendDoiToTitle(ObjectNode zenodoRecord, JsonNode itemData) {
        JsonNode titleNode = zenodoRecord.get(ZenodoMetaUtil.TITLE);
        if (titleNode != null && titleNode.isTextual()) {
            String doi = ZoteroUtil.getDOI(itemData);
            if (StringUtils.isNotBlank(doi)) {
                try {
                    URI uri = DOI.create(doi).toURI();
                    ZenodoMetaUtil.setValue(
                            zenodoRecord,
                            ZenodoMetaUtil.TITLE,
                            StringUtils.joinWith(" ", StringUtils.trim(titleNode.asText()), uri)
                    );
                } catch (MalformedDOIException ex) {
                    LOG.warn("found malformed DOI [" + doi + "]", ex);
                }
            }
        }
    }

    private boolean isPrimaryAttachmentOf(String zoteroAttachmentDownloadUrl, JsonNode itemData) {
        String primaryAttachment = itemData.at("/links/attachment/href").asText();
        return StringUtils.isNotBlank(primaryAttachment) && StringUtils.startsWith(zoteroAttachmentDownloadUrl, primaryAttachment);
    }

    private boolean hasPdfAttachment(JsonNode zoteroRecord) {
        boolean hasPdfAttachment = false;
        String itemType = zoteroRecord.at("/data/itemType").asText();
        if (StringUtils.equals("attachment", itemType)) {
            String contentType = zoteroRecord.at("/data/contentType").asText();
            if (StringUtils.equals("application/pdf", contentType)) {
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

            ZenodoMetaUtil.setCommunities(objectNode, communities.stream());

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_PART_OF, getProvenanceAnchor().getIRIString());
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PUBLICATION);
            ZenodoMetaUtil.setType(objectNode, "application/json");
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, reference.asText());

            List<String> creatorList = ZoteroUtil.parseCreators(creators);
            ZenodoMetaUtil.setCreators(objectNode, creatorList);

            List<String> tagValues = ZoteroUtil.parseKeywords(jsonNode);

            tagValues.forEach(tagValue -> ZenodoMetaUtil.addKeyword(objectNode, tagValue));

            String itemType = jsonNode.at("/data/itemType").asText();

            ZenodoMetaUtil.setValue(objectNode,
                    ZenodoMetaUtil.PUBLICATION_TYPE,
                    ZoteroUtil.ZOTERO_TO_ZENODO_PUB_TYPE_TRANSLATION_TABLE.getOrDefault(itemType, "other")
            );
            String dateStringParsed = ZoteroUtil.getPublicationDate(jsonNode);
            ZenodoMetaUtil.setPublicationDate(objectNode, dateStringParsed);
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TITLE, ZoteroUtil.getTitle(jsonNode));


            if (StringUtils.equals(itemType, ZoteroUtil.ZOTERO_JOURNAL_ARTICLE)) {
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_TITLE, ZoteroUtil.getJournalTitle(jsonNode));
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_VOLUME, ZoteroUtil.getJournalVolume(jsonNode));
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_ISSUE, ZoteroUtil.getJournalIssue(jsonNode));
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_PAGES, ZoteroUtil.getJournalPages(jsonNode));
            }

            if (Arrays.asList(ZoteroUtil.ZOTERO_BOOK, ZoteroUtil.ZOTERO_BOOK_SECTION).contains(itemType)) {
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.IMPRINT_PUBLISHER, ZoteroUtil.getPublisherName(jsonNode));
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.PARTOF_PAGES, ZoteroUtil.getJournalPages(jsonNode));
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.PARTOF_TITLE, ZoteroUtil.getBookTitle(jsonNode));
            }

            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_VARIANT_FORM_OF, ZoteroUtil.getDOI(jsonNode), ZenodoMetaUtil.PUBLICATION_TYPE_PUBLICATION);

            StringBuilder description = new StringBuilder();

            if (communities.stream().anyMatch(name -> StringUtils.contains(name, "batlit"))) {
                ZenodoMetaUtil.addKeyword(objectNode, "Biodiversity");
                ZenodoMetaUtil.addKeyword(objectNode, "Mammalia");
                ZenodoMetaUtil.addKeyword(objectNode, "Chiroptera");
                ZenodoMetaUtil.addKeyword(objectNode, "Chordata");
                ZenodoMetaUtil.addKeyword(objectNode, "Animalia");
                ZenodoMetaUtil.addKeyword(objectNode, "bats");
                ZenodoMetaUtil.addKeyword(objectNode, "bat");

                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_KINGDOM, "Animalia");
                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_PHYLUM, "Chordata");
                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CLASS, "Mammalia");
                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_ORDER, "Chiroptera");

                description.append("(Uploaded by Plazi for the Bat Literature Project) ");
            }

            if (communities.stream().anyMatch(
                    name -> StringUtils.containsIgnoreCase(name, "ipbes")
                            && StringUtils.containsIgnoreCase(name, "ias"))) {
                Stream.of("biodiversity",
                                "environment assessment",
                                "IPBES",
                                "Alien Invasive Species Assessment AIS",
                                "invasive species")
                        .forEach(keyword -> ZenodoMetaUtil.addKeyword(objectNode, keyword));

                ZenodoMetaUtil.appendIdentifier(
                        objectNode,
                        ZenodoMetaUtil.CITED_BY,
                        "10.5281/zenodo.7430682",
                        "publication-report"
                );

                description.append("(Uploaded by Plazi for the IPBES Invasive Alien Species Assessment) ");
            }

            String abstractNote = ZoteroUtil.getAbstract(jsonNode);

            String abstractString = StringUtils.isBlank(abstractNote)
                    ? "No abstract provided."
                    : abstractNote;
            description.append(abstractString);

            String descriptionString = description.toString();
            objectNode.put("description",
                    StringUtils.isBlank(descriptionString) ? "No description." : descriptionString
            );
        }
        return isLikelyZoteroRecord;
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
        StreamHandlerUtil.writeRecord(foundAtLeastOne, objectNode, getOutputStream());
    }

}
