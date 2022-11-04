package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdGetTest {

    private String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getSomething() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 1 || !toIRI(aContentHash).equals(key)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("some bits and bytes", StandardCharsets.UTF_8);
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.singletonList(RefNodeFactory.toIRI(aContentHash)));

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }

    @Test
    public void getSomethingSpecific() throws URISyntaxException, IOException {
        // reproduces https://github.com/bio-guoda/preston/issues/200
        URL resource = getClass().getResource("/bio/guoda/preston/store/issue-200-data/52/51/52513781cd5668b6c570120ebc576c075c40db09786a0788e501d1b34e17c402");

        URI uri = new File(resource.toURI()).getParentFile().getParentFile().getParentFile().toURI();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdGet cmdGet = new CmdGet();


        cmdGet.setRemotes(Collections.singletonList(uri));
        cmdGet.setContentIdsOrAliases(Collections.singletonList(toIRI("line:zip:hash://sha256/52513781cd5668b6c570120ebc576c075c40db09786a0788e501d1b34e17c402!/name.tsv!/L1")));

        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(cmdGet.getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory(HashType.sha256)), true, HashType.sha256);


        cmdGet.run(blobStore, cmdGet.getContentIdsOrAliases());
        assertThat(out.toString(), is("07070776-e643-47bf-afab-22da04e3fd9c\tLasioglossum (Chilalictus) nefrens Walker, 1995\tWalker\tspecies\t\tLasioglossum\tChilalictus\tnefrens\t\tICZN\testablished\t\t1995\t"));

    }

    @Test
    public void getVersion() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 1 || !toIRI(aContentHash).equals(key)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("some bits and bytes", StandardCharsets.UTF_8);
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        String statement = RefNodeFactory.toStatement(toIRI("blah"), RefNodeConstants.HAS_VERSION, toIRI(aContentHash)).toString();
        System.setIn(IOUtils.toInputStream(statement, StandardCharsets.UTF_8));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.emptyList());

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }

    @Test
    public void getPreviousVersion() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 1 || !toIRI(aContentHash).equals(key)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("some bits and bytes", StandardCharsets.UTF_8);
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        String statement = RefNodeFactory.toStatement(toIRI(aContentHash), RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("blah")).toString();
        System.setIn(IOUtils.toInputStream(statement, StandardCharsets.UTF_8));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.emptyList());

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }

    @Test
    public void getNothing() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("kaboom!");
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        System.setIn(IOUtils.toInputStream("<blah> <blah> <blah> .", StandardCharsets.UTF_8));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.emptyList());

        assertThat(out.toString(), is(""));
    }



}