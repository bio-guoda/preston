package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.zip.ZipEntry;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class DWCASignatureTest {

    @Test
    public void sha256Binary() throws IOException {
        InputStream is = dwcaInputStream("dwca-20180905.zip");
        IRI shaIRI = Hasher.calcHashIRI(is, NullOutputStream.NULL_OUTPUT_STREAM, HashType.sha256);
        assertThat(shaIRI.getIRIString(), is("hash://sha256/59f32445a50646d923f8ba462a7d87a848632f28bd93ac579de210e3375714de"));
    }

    @Test
    public void straightEMLShaDiff() throws IOException {
        IRI iri = Hasher.calcHashIRI(dwcaInputStream("dwca-20180905.zip"), NullOutputStream.NULL_OUTPUT_STREAM, HashType.sha256);
        assertThat(iri, is(not(Hasher.calcHashIRI(dwcaInputStream("dwca-20180916.zip"), NullOutputStream.NULL_OUTPUT_STREAM, HashType.sha256))));
    }

    @Test
    public void filteredEMLShaDiff() throws IOException {
        IRI shaIRIOld = emlSha(dwcaInputStream("dwca-20180905.zip"));
        IRI shaIRINew = emlSha(dwcaInputStream("dwca-20180916.zip"));
        assertThat(shaIRINew, is(shaIRIOld));
    }

    @Test
    public void invalidDwCALeadsToHashOfNothing() throws IOException {
        IRI shaIRIOld = emlSha(IOUtils.toInputStream("not a zipfile", StandardCharsets.UTF_8));
        String shaForEmptyContent = "hash://sha256/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assertThat(shaIRIOld, is(RefNodeFactory.toIRI(shaForEmptyContent)));
    }

    public IRI emlSha(InputStream dwcaInputStream) throws IOException {
        FileHasherTest.EntryFilter ignoreEML = new FileHasherTest.EntryFilter() {
            @Override
            public boolean accept(ZipEntry entry) {
                return !StringUtils.equals("eml.xml", entry.getName());
            }
        };
        Collection<String> hashesOld = FileHasherTest.hashDWCA(dwcaInputStream, ignoreEML);
        return Hasher.calcHashIRI(StringUtils.join(hashesOld, ""), HashType.sha256);
    }

    private InputStream dwcaInputStream(String name) {
        return getClass().getResourceAsStream(name);
    }


}