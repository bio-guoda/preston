package bio.guoda.preston.cmd;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Collections;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdCatTest {

    private String aContentHash = "hash://sha256/babababababababababababababababababababababababababababababababa";

    @Test
    public void getSomething() {
        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 1 || !toIRI(aContentHash).equals(key)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("some bits and bytes", Charset.defaultCharset());
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdCat cmdCat = new CmdCat();
        cmdCat.run(blobStoreNull, Collections.singletonList(aContentHash));

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
        assertThat(out.toString(), is("some bits and bytes"));
    }
}