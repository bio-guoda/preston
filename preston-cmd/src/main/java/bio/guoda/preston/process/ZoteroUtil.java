package bio.guoda.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

public class ZoteroUtil {
    public static String getZoteroAttachmentDownloadUrl(JsonNode zoteroArticleRecord) {
        String attachmentUrlString = null;
        JsonNode attachmentUrl = zoteroArticleRecord.at("/links/attachment/href");
        if (!attachmentUrl.isMissingNode() && StringUtils.isNotBlank(attachmentUrl.asText())) {

            attachmentUrlString = attachmentUrl.asText() + "/file/view";
        }
        return attachmentUrlString;
    }
}
