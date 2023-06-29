package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StopProcessingException;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ProxyInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

public class CmdGetTest {

    private String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getSomething() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull() {
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

        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(cmdGet.getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, HashType.sha256);


        cmdGet.run(blobStore, cmdGet.getContentIdsOrAliases());
        assertThat(out.toString(), is("07070776-e643-47bf-afab-22da04e3fd9c\tLasioglossum (Chilalictus) nefrens Walker, 1995\tWalker\tspecies\t\tLasioglossum\tChilalictus\tnefrens\t\tICZN\testablished\t\t1995\t"));

    }

    @Test
    public void getVersion() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull() {
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

        String statement = RefNodeFactory.toStatement(toIRI("foo:bar"), RefNodeConstants.HAS_VERSION, toIRI(aContentHash)).toString();
        System.setIn(IOUtils.toInputStream(statement, StandardCharsets.UTF_8));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.emptyList());

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }

    @Test
    public void getPreviousVersion() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull() {
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

        String statement = RefNodeFactory.toStatement(toIRI(aContentHash), RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("foo:bar")).toString();
        System.setIn(IOUtils.toInputStream(statement, StandardCharsets.UTF_8));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.emptyList());

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }

    @Test
    public void getNothing() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull() {
            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("kaboom!");
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        System.setIn(IOUtils.toInputStream("<foo:bar> <foo:bar> <foo:bar> .", StandardCharsets.UTF_8));

        CmdGet cmdGet = new CmdGet();
        cmdGet.run(blobStoreNull, Collections.emptyList());

        assertThat(out.toString(), is(""));
    }

    @Test
    public void getPartialNoCacheStopProcessing() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        final CmdGet cmd = new CmdGet();
        cmd.setOutputStream(out);
        cmd.setCacheEnabled(false);
        cmd.setContentIdsOrAliases(Collections.singletonList(toIRI("hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03")));
        AtomicBoolean askedToStop = new AtomicBoolean(false);

        ProxyInputStream stoppingStream = new ProxyInputStream(IOUtils.toInputStream("hello world", StandardCharsets.UTF_8)) {

            @Override
            protected void afterRead(final int n) throws IOException {
                if (askedToStop.get()) {
                    throw new IOException("asked to stop, but still processing anyway");
                }

                if (cmd.shouldKeepProcessing()) {
                    cmd.stopProcessing();
                    askedToStop.set(true);
                }
            }
        };


        BlobStoreNull blobStoreWithStop = new BlobStoreNull() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return stoppingStream;
            }
        };

        try {
            cmd.run(blobStoreWithStop);
        } catch (RuntimeException ex) {
            Throwable cause = ex.getCause();
            assertThat(cause, instanceOf(StopProcessingException.class));
        }

        assertThat(cmd.shouldKeepProcessing(), is(false));
        assertThat(askedToStop.get(), is(true));
    }

    @Test
    public void retrieveContentByAlias() throws URISyntaxException {
        // see https://github.com/bio-guoda/preston/issues/248
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        final CmdGet cmd = new CmdGet();
        String path = "/bio/guoda/preston/cmd/alias/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a";
        File resource = new File(getClass().getResource(path).toURI());
        File dataDir = resource.getParentFile().getParentFile().getParentFile();

        cmd.setLocalDataDir(dataDir.getAbsolutePath());
        cmd.setOutputStream(out);
        cmd.setContentIdsOrAliases(Collections.singletonList(toIRI("https://bing.com")));

        cmd.run();

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                not(containsString("duckduckgo.com")));
        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                containsString("bing.com"));


    }


}