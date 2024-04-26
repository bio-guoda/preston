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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ALTERNATE_OF;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderZoteroTest {

    public static final Pattern URL_PATTERN_ZOTERO_GROUP = Pattern.compile("http[s]{0,1}://(www\\.){0,1}(zotero\\.org/groups/)(?<groupIdOrName>[^/]+)(/.*){0,1}");

    @Test
    public void onItem() throws IOException {
        //
        // retrieved from address below on 2024-04-23
        //
        // https://api.zotero.org/groups/5435545/items?key=[SUPER SECRET]
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

    @Test
    public void onGroup() throws IOException {
        //
        // retrieved from address below on 2024-04-26
        //
        // https://api.zotero.org/groups/5435545?key=[SUPER SECRET]
        //
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("/bio/guoda/preston/process/zotero/group.json"));
        JsonNode groupIdString = jsonNode.at("/id");
        JsonNode numItems = jsonNode.at("/meta/numItems");
        long numberOfItems = numItems.longValue();
        assertThat(numberOfItems, is(12574L));
        long groupId = groupIdString.longValue();
        assertThat(groupId, is(5435545L));

        int pageSize = 100;
        Stream<Quad> statements = LongStream.rangeClosed(0, numberOfItems / pageSize)
                .mapToObj(page -> "https://api.zotero.org/groups/" + groupId + "/items?start=" + page * pageSize + "&limit=100")
                .map(url ->
                        RefNodeFactory.toStatement(
                                RefNodeFactory.toIRI(url),
                                HAS_VERSION,
                                RefNodeFactory.toBlank()));

        List<Quad> list = statements.collect(Collectors.toList());

        assertThat(list.size(), is(126));
        assertThat(list.get(0).getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items?start=0&limit=100>"));

        assertThat(list.get(list.size() - 1).getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items?start=12500&limit=100>"));

    }

    @Test
    public void onZoteroGroupUrl() {
        String url = "https://www.zotero.org/groups/5435545/bat_literature_project";

        List<Quad> statements = new ArrayList<>();

        StatementEmitter statementEmitter = new StatementEmitter() {

            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }

        };

        String groupIdOrName = getGroupIdOrName(url);
        if (StringUtils.isNotBlank(groupIdOrName)) {
            String zoteroGroupRequest = "https://api.zotero.org/groups/" + groupIdOrName;
            statementEmitter.emit(RefNodeFactory.toStatement(
                    RefNodeFactory.toIRI(zoteroGroupRequest),
                    ALTERNATE_OF,
                    RefNodeFactory.toIRI(url)
            ));
            statementEmitter.emit(RefNodeFactory.toStatement(
                    RefNodeFactory.toIRI(zoteroGroupRequest),
                    HAS_VERSION,
                    RefNodeFactory.toBlank()
            ));

        }

        assertThat(statements.size(), is(2));


    }

    @Test
    public void urlPatternForGroup1() {
        assertThat(
                getGroupIdOrName("https://www.zotero.org/groups/5435545/bat_literature_project"),
                is("5435545")
        );
    }

    @Test
    public void urlPatternForGroup2() {
        assertThat(
                getGroupIdOrName("https://zotero.org/groups/5435545/bat_literature_project"),
                is("5435545")
        );
    }

    @Test
    public void urlPatternForGroup3() {
        assertThat(
                getGroupIdOrName("http://www.zotero.org/groups/5435545/bat_literature_project"),
                is("5435545")
        );
    }

    @Test
    public void urlPatternForGroup4() {
        assertThat(
                getGroupIdOrName("http://www.zotero.org/groups/bat_literature_project"),
                is("bat_literature_project")
        );
    }

    private String getGroupIdOrName(String urlStringCandidate) {
        Matcher matcher = URL_PATTERN_ZOTERO_GROUP
                .matcher(urlStringCandidate);
        return matcher.matches() ? matcher.group("groupIdOrName") : null;
    }


}