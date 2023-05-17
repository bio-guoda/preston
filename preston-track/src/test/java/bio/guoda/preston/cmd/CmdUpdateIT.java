package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class CmdUpdateIT {
    
    @Test
    public void doUpdateWithSeeds() throws IOException {
        BlobStoreNull blobStore = new BlobStoreNull();
        HexaStoreNull logRelations = new HexaStoreNull();
        assertThat(blobStore.putAttemptCount.get(), Is.is(0));
        assertThat(logRelations.putLogVersionAttemptCount.get(), Is.is(0));

        CmdUpdate cmdUpdate = new CmdUpdate();
        cmdUpdate.setSeeds(Arrays.asList(Seeds.IDIGBIO, Seeds.GBIF, Seeds.BIOCASE));
        cmdUpdate.run(
                blobStore,
                blobStore, logRelations);

        assertTrackedActivity(blobStore, logRelations);
    }


    @Test
    public void doTrackURL() throws IOException {
        BlobStoreNull blobStore = new BlobStoreNull();
        HexaStoreNull logRelations = new HexaStoreNull();
        assertThat(blobStore.putAttemptCount.get(), Is.is(0));
        assertThat(logRelations.putLogVersionAttemptCount.get(), Is.is(0));

        CmdUpdate cmdUpdate = new CmdUpdate();
        cmdUpdate.setIRIs(Arrays.asList(RefNodeFactory.toIRI("https://example.org")));
        cmdUpdate.run(
                blobStore,
                blobStore,
                logRelations
        );

        assertTrackedActivity(blobStore, logRelations);
    }

    private void assertTrackedActivity(BlobStoreNull blobStore, HexaStoreNull logRelations) throws IOException {
        assertThat(blobStore.putAttemptCount.get() > 0, Is.is(true));
        assertThat(blobStore.mostRecentBlob.size() > 0, Is.is(true));
        assertThat(logRelations.putLogVersionAttemptCount.get() > 0, Is.is(true));

        String provenanceLog = IOUtils.toString(blobStore.mostRecentBlob.toByteArray(), StandardCharsets.UTF_8.toString());

        assertThat(provenanceLog, startsWith("<https://preston.guoda.bio> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent>"));

        assertThat(provenanceLog, not(containsString(" _:")));

        List<Quad> quads = RDFUtil.parseQuads(IOUtils.toInputStream(provenanceLog, StandardCharsets.UTF_8));

        List<String> graphNamesIgnoreDefault = quads.stream()
                .filter(x -> x.getGraphName().isPresent())
                .map(x -> ((IRI) x.getGraphName().get()).getIRIString())
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