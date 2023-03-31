package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class CmdAppendTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void doProcessNothing() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull();
        HexaStoreNull statementStoreNull = new HexaStoreNull();

        CmdAppend cmdAppend = new CmdAppend();
        cmdAppend.setInputStream(new ByteArrayInputStream("".getBytes()));
        cmdAppend.run(blobStoreNull, statementStoreNull);

        assertThat(blobStoreNull.putAttemptCount.get() > 0, Is.is(true));
        assertThat(statementStoreNull.putLogVersionAttemptCount.get() > 0, Is.is(true));
        assertThat(blobStoreNull.mostRecentBlob.size() > 0, Is.is(true));

        String provenanceLog = IOUtils.toString(
                blobStoreNull.mostRecentBlob.toByteArray(),
                StandardCharsets.UTF_8.toString()
        );

        long numberOfActivities = 1L;

        assertNumberOfActivities(provenanceLog, numberOfActivities);
    }

    @Test
    public void doProcessABlankVersionWithGraphName() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull();
        HexaStoreNull statementStoreNull = new HexaStoreNull();

        CmdAppend cmdAppend = new CmdAppend();

        Quad quad = RefNodeFactory.toStatement(
                RefNodeFactory.toIRI(UUID.fromString("0b4201b7-8ca4-4367-a7be-e9e0c7b8bc8c")),
                RefNodeFactory.toIRI(URI.create("https://example.org")),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toBlank());

        cmdAppend.setInputStream(new ByteArrayInputStream(quad.toString().getBytes()));

        cmdAppend.run(
                blobStoreNull,
                statementStoreNull
        );

        assertThat(blobStoreNull.putAttemptCount.get() > 0, Is.is(true));
        assertThat(statementStoreNull.putLogVersionAttemptCount.get() > 0, Is.is(true));
        assertThat(blobStoreNull.mostRecentBlob.size() > 0, Is.is(true));

        String provenanceLog = IOUtils.toString(
                blobStoreNull.mostRecentBlob.toByteArray(),
                StandardCharsets.UTF_8.toString());

        assertNumberOfActivities(provenanceLog, 1L);
    }

    @Test
    public void doProcessAStatementWithoutABlankStatement() throws IOException {
        BlobStoreNull blobStoreNull = new BlobStoreNull();
        HexaStoreNull statementStoreNull = new HexaStoreNull();

        CmdAppend cmdAppend = new CmdAppend();

        Quad quad = RefNodeFactory.toStatement(
                RefNodeFactory.toIRI(UUID.fromString("1dd4573c-a332-478e-bd66-93de617857bc")),
                RefNodeFactory.toIRI(URI.create("https://example.org")),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI(URI.create("https://example.org")));

        cmdAppend.setInputStream(new ByteArrayInputStream(quad.toString().getBytes()));

        cmdAppend.run(
                blobStoreNull,
                statementStoreNull
        );

        assertThat(blobStoreNull.putAttemptCount.get() > 0, Is.is(true));
        assertThat(statementStoreNull.putLogVersionAttemptCount.get() > 0, Is.is(true));
        assertThat(blobStoreNull.mostRecentBlob.size() > 0, Is.is(true));

        String provenanceLog = IOUtils.toString(
                blobStoreNull.mostRecentBlob.toByteArray(),
                StandardCharsets.UTF_8.toString());

        assertNumberOfActivities(provenanceLog, 1L);
    }

    public void assertNumberOfActivities(String provenanceLog, long numberOfActivities) {
        assertThat(provenanceLog, startsWith("<https://preston.guoda.bio> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent>"));

        assertThat(provenanceLog, not(containsString(" _:")));

        List<Quad> quads = RDFUtil.parseQuads(IOUtils.toInputStream(provenanceLog, StandardCharsets.UTF_8));

        List<String> graphNamesIgnoreDefault = quads.stream()
                .filter(x -> x.getGraphName().isPresent())
                .map(x -> ((IRI) x.getGraphName().get()).getIRIString())
                .filter(x -> !StringUtils.equals(x, "urn:x-arq:DefaultGraphNode"))
                .collect(Collectors.toList());

        assertThat(graphNamesIgnoreDefault.size(), is(quads.size()));

        List<String> activityUUIDs = quads.stream()
                .filter(x -> x.getPredicate().equals(RefNodeConstants.IS_A) && x.getObject().equals(RefNodeConstants.ACTIVITY))
                .map(x -> ((IRI) x.getSubject()).getIRIString())
                .collect(Collectors.toList());

        long numActivities = activityUUIDs.stream()
                .map(x -> RegExUtils.replacePattern(x, "^" + RefNodeConstants.URN_UUID_PREFIX, ""))
                .map(UUID::fromString)
                .distinct()
                .count();
        assertThat(numActivities, is(numberOfActivities));

        long numInformedActivities = quads.stream()
                .filter(x -> x.getPredicate().equals(WAS_INFORMED_BY))
                .count();
        assertThat(numInformedActivities, is(numActivities - 1));
    }

    @Test
    public void append() {
        String absolutePath = folder.getRoot().getAbsolutePath();
        verifyAppend(absolutePath);
        assertDepth(absolutePath, 1);

    }

    @Test
    public void appendTwice() {
        String absolutePath = folder.getRoot().getAbsolutePath();
        verifyAppend(absolutePath);
        verifyAppend(absolutePath);
        assertDepth(absolutePath, 2);
    }

    @Test
    public void appendThreeTimes() {
        String absolutePath = folder.getRoot().getAbsolutePath();
        verifyAppend(absolutePath);
        verifyAppend(absolutePath);
        verifyAppend(absolutePath);
        assertDepth(absolutePath, 3);

    }

    public void assertDepth(String absolutePath, int expectedHistoryDepth) {
        CmdHistory history = new CmdHistory();
        history.setLocalDataDir(absolutePath);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        history.setOutputStream(outputStream);
        history.run();
        String[] lines = StringUtils.split(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), "\n");
        assertThat(lines.length, is(expectedHistoryDepth));
    }

    public void verifyAppend(String absolutePath) {
        CmdAppend cmd = new CmdAppend();
        cmd.setLocalDataDir(absolutePath);

        String nquadToBeAppended = "<foo:bar> <some:iri> <foo:bar> .";
        cmd.setInputStream(IOUtils.toInputStream(nquadToBeAppended, StandardCharsets.UTF_8));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmd.setOutputStream(outputStream);

        cmd.run();

        String[] lines = StringUtils.split(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), "\n");

        assertThat(lines.length, is(greaterThan(0)));
        assertThat(lines[lines.length - 1], is(nquadToBeAppended));
    }


}