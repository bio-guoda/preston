package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.fail;

public class CmdAliasTest {

    @Test(expected = IllegalArgumentException.class)
    public void invalidAlias() {
        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca");
        IRI contentId = RefNodeFactory.toIRI("hash://sha256/f0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.run(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                fail("should not work");
            }
        });

    }

    @Test
    public void validAlias() {
        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("myfinalpaper.txt");
        IRI contentId = RefNodeFactory.toIRI("hash://sha256/f0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.run(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                fail("should not work");
            }
        });

    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAliasTarget() {
        CmdAlias cmdAlias = new CmdAlias();
        IRI alias = RefNodeFactory.toIRI("myfinalpaper.txt");
        IRI contentId = RefNodeFactory.toIRI("foo:bar:123");
        cmdAlias.setParams(Arrays.asList(alias, contentId));
        cmdAlias.run(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                fail("should not work");
            }
        });

    }

    @Test
    public void showTrackedAlias() {

        KeyValueStore testPersistence = TestUtil.getTestPersistence();
        final HexaStoreImpl provIndex = new HexaStoreImpl(testPersistence, HashType.sha256);
        BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(testPersistence, true, HashType.sha256);

        CmdTrack track = new CmdTrack();
        track.setIRIs(Arrays.asList(RefNodeFactory.toIRI("https://example.org")));
        track.run(blobStore, provIndex);

        CmdAlias cmdAlias = new CmdAlias();
        cmdAlias.run(blobStore, provIndex);

    }


}