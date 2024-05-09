package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ZoteroUtil;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ZoteroFileStreamHandler implements ContentStreamHandler {


    private final Persisting persisting;
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public ZoteroFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                   OutputStream os,
                                   Persisting persisting,
                                   Dereferencer<InputStream> dereferencer) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.persisting = persisting;
        this.dereferencer = dereferencer;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            JsonNode candidateContent = new ObjectMapper().readTree(is);
            if (candidateContent.isArray()) {
                for (JsonNode candidateArticleItem : candidateContent) {
                    handleZoteroItem(foundAtLeastOne, iriString, candidateArticleItem);
                }
            } else {
                handleZoteroItem(foundAtLeastOne, iriString, candidateContent);
            }

        } catch (IOException e) {
            // opportunistic parsing, so ignore exceptions
        }
        return foundAtLeastOne.get();
    }

    private void handleZoteroItem(AtomicBoolean foundAtLeastOne, String iriString, JsonNode jsonNode) throws ContentStreamException, IOException {
        boolean likelyZoteroRecord;
        JsonNode reference = jsonNode.at("/links/self/href");
        JsonNode creators = jsonNode.at("/data/creators");
        likelyZoteroRecord = !reference.isMissingNode()
                && !creators.isMissingNode()
                && StringUtils.contains(reference.asText(), "zotero.org");
        if (likelyZoteroRecord) {
            ObjectNode objectNode = new ObjectMapper().createObjectNode();

            ZenodoMetaUtil.setCommunities(objectNode, Stream.of("batlit", "biosyslit"));

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, iriString);
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TYPE, "application/json+zotero");
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, reference.asText());

            List<String> creatorList = new ArrayList<>();
            if (creators.isArray()) {
                for (JsonNode creator : creators) {
                    if (creator.has("firstName") && creator.has("lastName")) {
                        creatorList.add(creator.get("lastName").asText() + ", " + creator.get("firstName").asText());
                    }
                }
            }
            ZenodoMetaUtil.setCreators(objectNode, creatorList);

            if (StringUtils.equals(jsonNode.at("/data/itemType").asText(), "journalArticle")) {
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
                ZenodoMetaUtil.setPublicationDate(objectNode, jsonNode.at("/data/date").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_TITLE, jsonNode.at("/data/publicationTitle").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TITLE, jsonNode.at("/data/title").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_VOLUME, jsonNode.at("/data/volume").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_ISSUE, jsonNode.at("/data/issue").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_PAGES, jsonNode.at("/data/pages").asText());
            }

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.DOI, jsonNode.at("/data/DOI").asText());

            String value = jsonNode.at("/links/attachment/href").asText();
            if (StringUtils.isNoneBlank(value)) {
                String zoteroAttachmentDownloadUrl = ZoteroUtil.getZoteroAttachmentDownloadUrl(jsonNode);
                ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, zoteroAttachmentDownloadUrl);
                try {
                    IRI downloadIRI = RefNodeFactory.toIRI(zoteroAttachmentDownloadUrl);
                    InputStream attachementInputStream = ContentQueryUtil.getContent(dereferencer, downloadIRI, persisting);
                    if (attachementInputStream == null) {
                        throw new ContentStreamException("cannot generate Zenodo record due to unresolved attachment [" + zoteroAttachmentDownloadUrl + "]");
                    }
                    IRI contentId = Hasher.calcHashIRI(
                            attachementInputStream,
                            NullOutputStream.INSTANCE,
                            HashType.md5
                    );
                    ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, contentId.getIRIString());
                } catch (IOException e) {
                    throw new ContentStreamException("cannot generate Zenodo record due to unresolved attachment [" + zoteroAttachmentDownloadUrl + "]", e);
                }

            }

            ZenodoMetaUtil.addKeyword(objectNode, "Biodiversity");
            ZenodoMetaUtil.addKeyword(objectNode, "Mammalia");
            ZenodoMetaUtil.addKeyword(objectNode, "Chiroptera");
            ZenodoMetaUtil.addKeyword(objectNode, "Chordata");
            ZenodoMetaUtil.addKeyword(objectNode, "Animalia");

            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_KINGDOM, "Animalia");
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_PHYLUM, "Chordata");
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CLASS, "Mammalia");
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_ORDER, "Chiroptera");

            foundAtLeastOne.set(true);
            writeRecord(foundAtLeastOne, objectNode);
        }
    }


    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        objectNode.put("description", "Uploaded by Plazi for Bat Literature Project. We do not have abstracts.");
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
