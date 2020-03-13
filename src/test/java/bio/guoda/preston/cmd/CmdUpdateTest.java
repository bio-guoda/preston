package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

public class CmdUpdateTest {

    private static final AtomicInteger putAttemptCount = new AtomicInteger(0);
    private static final AtomicInteger putLogVersionAttemptCount = new AtomicInteger(0);
    private static ByteArrayOutputStream mostRecentBlob;

    @Test
    public void doUpdate() throws IOException {
        assertThat(putAttemptCount.get(), Is.is(0));
        assertThat(putLogVersionAttemptCount.get(), Is.is(0));
        new CmdUpdate().run(
                new BlobStoreNull(),
                new StatementStoreNull());
        assertThat(putAttemptCount.get() > 0, Is.is(true));
        assertThat(putLogVersionAttemptCount.get() > 0, Is.is(true));
        assertThat(mostRecentBlob.size() > 0, Is.is(true));

        String provenanceLog = IOUtils.toString(mostRecentBlob.toByteArray(), StandardCharsets.UTF_8.toString());

        assertThat(provenanceLog, startsWith("<https://preston.guoda.bio> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent>"));

        assertThat(provenanceLog, not(containsString(" _:")));

        Stream<org.apache.commons.rdf.api.Quad> quads = RDFUtil.asQuadStream(IOUtils.toInputStream(provenanceLog, StandardCharsets.UTF_8));

        Stream<String> graphNamesIgnoreDefault = quads
                .filter(x -> x.getGraphName().isPresent())
                .map(x -> ((IRI)x.getGraphName().get()).getIRIString())
                .filter(x -> !StringUtils.equals(x, "urn:x-arq:DefaultGraphNode"));

        long count = graphNamesIgnoreDefault
                .map(UUID::fromString)
                .distinct()
                .count();
        assertThat(count, is(1L));
    }


    private static class BlobStoreNull implements BlobStore {

        @Override
        public IRI put(InputStream is) throws IOException {
            mostRecentBlob = new ByteArrayOutputStream();
            IOUtils.copy(is, mostRecentBlob);
            putAttemptCount.incrementAndGet();
            return null;
        }

        @Override
        public InputStream get(IRI key) throws IOException {
            return null;
        }
    }

    private static class StatementStoreNull implements StatementStore {
        @Override
        public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
            putLogVersionAttemptCount.incrementAndGet();
        }

        @Override
        public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
            return null;
        }
    }
}