package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import static bio.guoda.preston.cmd.ZenodoMetaUtil.PUBLICATION_DATE;

public class DarkTaxonUtil {
    public static final String LSID_PREFIX = "urn:lsid:github.com:darktaxon:";

    static void appendAlternateIdentifiers(ObjectNode linkRecords, String imageContentId) {
        ZenodoMetaUtil.appendIdentifier(linkRecords, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, imageContentId);
        ZenodoMetaUtil.appendIdentifier(linkRecords, ZenodoMetaUtil.HAS_VERSION, imageContentId);
    }

    public static void populatePhotoDepositMetadata(ObjectNode objectNode, String imageFilename, String specimenId, String imageContentId, String mimeType, PublicationDateFactory publicationDateFactory, List<String> communities, String title, String description) {
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CATALOG_NUMBER, specimenId);
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_INSTITUTION_CODE, "MfN");
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_MATERIAL_SAMPLE_ID, specimenId);
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_AC_ASSOCIATED_SPECIMEN, specimenId);
        objectNode.put(ZenodoMetaUtil.TITLE, title);
        setDescription(objectNode, description);
        ZenodoMetaUtil.setFilename(objectNode, imageFilename);
        appendAlternateIdentifiers(objectNode, imageContentId);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, StringUtils.startsWith(specimenId, "urn:lsid:") ? specimenId : LSID_PREFIX + specimenId);
        ZenodoMetaUtil.setType(objectNode, mimeType);
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_IMAGE);
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.IMAGE_TYPE, ZenodoMetaUtil.IMAGE_TYPE_PHOTO);
        ZenodoMetaUtil.setCreators(objectNode, Arrays.asList("Museum für Naturkunde Berlin"));
        ZenodoMetaUtil.setValue(objectNode, PUBLICATION_DATE, publicationDateFactory.getPublicationDate());
        ZenodoMetaUtil.setCommunities(objectNode, communities.stream());
    }

    public static void setDescription(ObjectNode objectNode, String description) {
        objectNode.put("description", description);
    }
}
