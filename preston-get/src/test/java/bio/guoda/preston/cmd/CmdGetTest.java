package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdGetTest {

    private String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getSomething() {
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
    public void getVersion() {
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
    public void getPreviousVersion() {
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
    public void getNothing() {
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