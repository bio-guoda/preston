package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderZoteroTest {

    @Test
    public void onItem() throws IOException {
        //
        // retrieved from address below on 2024-04-23
        //
        // https://api.zotero.org/groups/5435545?key=[SUPER SECRET]
        //
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("/bio/guoda/preston/process/zotero/group-items.json"));
        List<Quad> statements = new ArrayList<>();

        assertThat(jsonNode.size(), is(25));

        for (JsonNode item : jsonNode) {
            JsonNode itemUrl = item.at("/links/self/href");
            if (itemUrl != null) {
                JsonNode attachmentUrl = item.at("/links/attachment/href");
                if (attachmentUrl != null && StringUtils.isNotBlank(attachmentUrl.asText())) {
                    statements.add(RefNodeFactory.toStatement(
                            RefNodeFactory.toIRI(itemUrl.asText()),
                            HAS_VERSION,
                            RefNodeFactory.toBlank())
                    );
                    statements.add(RefNodeFactory.toStatement(
                            RefNodeFactory.toIRI(attachmentUrl.asText() + "/file/view"),
                            HAS_VERSION,
                            RefNodeFactory.toBlank())
                    );
                }
            }
        }

        assertThat(statements.size(), is(24));

        Quad item = statements.get(0);
        assertThat(item.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/45IC4D9G>"));
        assertThat(item.getPredicate(), is(HAS_VERSION));
        assertTrue(RefNodeFactory.isBlankOrSkolemizedBlank(item.getObject()));

        Quad itemAttachment = statements.get(1);
        assertThat(itemAttachment.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/I5ED2F3N/file/view>"));
        assertThat(itemAttachment.getPredicate(), is(HAS_VERSION));
        assertTrue(RefNodeFactory.isBlankOrSkolemizedBlank(itemAttachment.getObject()));

        Quad item1 = statements.get(2);
        assertThat(item1.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/C2P2IBEI>"));
        assertThat(item1.getPredicate(), is(HAS_VERSION));
        assertTrue(RefNodeFactory.isBlankOrSkolemizedBlank(item1.getObject()));

        Quad item1Attachment = statements.get(3);
        assertThat(item1Attachment.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/Z3L7EK3H/file/view>"));
        assertThat(item1Attachment.getPredicate(), is(HAS_VERSION));
        assertTrue(RefNodeFactory.isBlankOrSkolemizedBlank(item1Attachment.getObject()));

    }


}