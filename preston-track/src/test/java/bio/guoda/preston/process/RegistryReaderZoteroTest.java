package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderZoteroTest {

    @Test
    public void on() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderZotero registryReaderZotero = new RegistryReaderZotero(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return null;
            }
        }, new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderZotero.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://www.zotero.org/groups/5435545"),
                RefNodeFactory.toIRI("http://purl.org/pav/hasVersion"),
                RefNodeFactory.toIRI("hash://sha256/8a48d091d0637aefe86d837918f5fa569d8dfd14cb1c7e042c327064c81a8149"))
        );

        assertThat(statements.size(), is(2));
    }

    @Test
    public void onAttachment() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderZotero registryReaderZotero = new RegistryReaderZotero(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        }, new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderZotero.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://api.zotero.org/groups/5435545/items/REYQJNPD/file/view"),
                RefNodeFactory.toIRI("http://purl.org/pav/hasVersion"),
                RefNodeFactory.toIRI("hash://sha256/8a48d091d0637aefe86d837918f5fa569d8dfd14cb1c7e042c327064c81a8149"))
        );

        assertThat(statements.size(), is(0));
    }

    @Test
    public void onItemsVersion() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderZotero registryReaderZotero = new RegistryReaderZotero(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("/bio/guoda/preston/process/zotero/group-items.json");
            }
        }, new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReaderZotero.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://api.zotero.org/groups/5435545/items"),
                RefNodeFactory.toIRI("http://purl.org/pav/hasVersion"),
                RefNodeFactory.toIRI("hash://sha256/8db70f1d4eada90e06851ef6d7552e91ec11a7af99f2e30b53635abf462391ab"))
        );

        assertThat(statements.size(), is(99));

        assertThat(statements.get(96),
                is(RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI("cut:hash://sha256/8db70f1d4eada90e06851ef6d7552e91ec11a7af99f2e30b53635abf462391ab!/b66726-69016"),
                        HAS_FORMAT,
                        RefNodeFactory.toLiteral("application/json+zotero")))
        );

        assertThat(statements.get(97),
                is(RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI("hash://sha256/8db70f1d4eada90e06851ef6d7552e91ec11a7af99f2e30b53635abf462391ab"),
                        RefNodeConstants.HAD_MEMBER,
                        RefNodeFactory.toIRI("cut:hash://sha256/8db70f1d4eada90e06851ef6d7552e91ec11a7af99f2e30b53635abf462391ab!/b66726-69016")))
        );
        assertThat(statements.get(98).getSubject(),
                Is.is(RefNodeFactory.toIRI("cut:hash://sha256/8db70f1d4eada90e06851ef6d7552e91ec11a7af99f2e30b53635abf462391ab!/b66726-69016")));

        assertThat(statements.get(98).getPredicate(),
                Is.is(HAS_VERSION));
        assertThat(RefNodeFactory.isBlankOrSkolemizedBlank(statements.get(98).getObject()),
                Is.is(true));

    }

    @Test
    public void onItem() throws IOException {
        //
        // retrieved from address below on 2024-04-23
        //
        // https://api.zotero.org/groups/5435545/items?key=[SUPER SECRET]
        //
        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/process/zotero/group-items.json");
        List<Quad> statements = new ArrayList<>();

        StatementEmitter emitter = new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        };

        RegistryReaderZotero.requestItemAttachments(is, emitter);

        assertThat(statements.size(), is(24));


        Quad itemFormat = statements.get(0);
        assertThat(itemFormat.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/I5ED2F3N/file/view>"));
        assertThat(itemFormat.getPredicate(), is(HAS_FORMAT));
        assertThat(itemFormat.getObject(), is(RefNodeFactory.toLiteral("application/pdf")));

        Quad itemAttachment = statements.get(1);
        assertThat(itemAttachment.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/I5ED2F3N/file/view>"));
        assertThat(itemAttachment.getPredicate(), is(HAS_VERSION));
        assertTrue(RefNodeFactory.isBlankOrSkolemizedBlank(itemAttachment.getObject()));

        Quad item1Attachment = statements.get(2);
        assertThat(item1Attachment.getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items/Z3L7EK3H/file/view>"));
        assertThat(item1Attachment.getPredicate(), is(HAS_FORMAT));
        assertThat(itemFormat.getObject(), is(RefNodeFactory.toLiteral("application/pdf")));

    }

    @Test
    public void onGroup() throws IOException {
        //
        // retrieved from address below on 2024-04-26
        //
        // https://api.zotero.org/groups/5435545?key=[SUPER SECRET]
        //
        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/process/zotero/group.json");

        List<Quad> statements = new ArrayList<>();

        StatementEmitter emitter = new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        };

        RegistryReaderZotero.emitRequestForItems(is, emitter);

        assertThat(statements.size(), is(126));
        assertThat(statements.get(0).getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items?start=0&limit=100>"));

        assertThat(statements.get(statements.size() - 1).getSubject().ntriplesString(), is("<https://api.zotero.org/groups/5435545/items?start=12500&limit=100>"));

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

        RegistryReaderZotero.requestGroupMetadata(url, statementEmitter);

        assertThat(statements.size(), is(2));


    }

    @Test
    public void urlPatternForGroup1() {
        assertThat(
                RegistryReaderZotero.getGroupIdOrName("https://www.zotero.org/groups/5435545/bat_literature_project"),
                is("5435545")
        );
    }

    @Test
    public void urlPatternForGroup2() {
        assertThat(
                RegistryReaderZotero.getGroupIdOrName("https://zotero.org/groups/5435545/bat_literature_project"),
                is("5435545")
        );
    }

    @Test
    public void urlPatternForGroup3() {
        assertThat(
                RegistryReaderZotero.getGroupIdOrName("http://www.zotero.org/groups/5435545/bat_literature_project"),
                is("5435545")
        );
    }

    @Test
    public void urlPatternForGroup4() {
        assertThat(
                RegistryReaderZotero.getGroupIdOrName("http://www.zotero.org/groups/bat_literature_project"),
                is("bat_literature_project")
        );
    }


}