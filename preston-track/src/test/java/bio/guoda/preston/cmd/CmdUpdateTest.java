package bio.guoda.preston.cmd;

import bio.guoda.preston.rdf.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdUpdateTest {


    @Test
    public void doUpdate() throws IOException {
        BlobStoreNull blobStore = new BlobStoreNull();
        HexaStoreNull logRelations = new HexaStoreNull();
        assertThat(blobStore.putAttemptCount.get(), Is.is(0));
        assertThat(logRelations.putLogVersionAttemptCount.get(), Is.is(0));

        new CmdUpdate().run(
                blobStore,
                logRelations);

        assertThat(blobStore.putAttemptCount.get() > 0, Is.is(true));
        assertThat(blobStore.mostRecentBlob.size() > 0, Is.is(true));
        assertThat(logRelations.putLogVersionAttemptCount.get() > 0, Is.is(true));

        String provenanceLog = IOUtils.toString(blobStore.mostRecentBlob.toByteArray(), StandardCharsets.UTF_8.toString());

        assertThat(provenanceLog, startsWith("<https://preston.guoda.bio> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent>"));

        assertThat(provenanceLog, not(containsString(" _:")));

        List<Quad> quads = RDFUtil.asQuadStream(IOUtils.toInputStream(provenanceLog, StandardCharsets.UTF_8)).collect(Collectors.toList());

        List<String> graphNamesIgnoreDefault = quads.stream()
                .filter(x -> x.getGraphName().isPresent())
                .map(x -> ((IRI)x.getGraphName().get()).getIRIString())
                .filter(x -> !StringUtils.equals(x, "urn:x-arq:DefaultGraphNode")).collect(Collectors.toList());

        assertThat(graphNamesIgnoreDefault.size(), is(quads.size()));

        long numActivities = graphNamesIgnoreDefault.stream()
                .map(x -> RegExUtils.replacePattern(x, "^" + RefNodeConstants.URN_UUID_PREFIX, ""))
                .map(UUID::fromString)
                .distinct()
                .count();
        assertThat(numActivities, greaterThan(1L));

        long numInformedActivities = quads.stream()
                .filter(x -> x.getPredicate().equals(WAS_INFORMED_BY))
                .count();
        assertThat(numInformedActivities, is(numActivities - 1));
    }


}