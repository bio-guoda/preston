package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.nullValue;

public class ZenodoMetadataFileStreamHandlerIT {

    private ZenodoContext ctx = null;

    @Before
    public void create() throws IOException {
        String accessToken = ZenodoTestUtil.getAccessToken();
        assertThat(accessToken, is(not(isEmptyString())));
        ctx = new ZenodoContext(accessToken, "https://sandbox.zenodo.org");
    }

    @After
    public void delete() throws IOException {
        try {
            cleanupPreExisting();
        } catch (IOException ex) {
            // ignore
        }
    }

    private void cleanupPreExisting() throws IOException {
        Collection<Pair<Long, String>> byAlternateIds = ZenodoUtils.findRecordsByAlternateIds(ctx, Arrays.asList(getContentId()), "", uri -> ResourcesHTTP.asInputStream(uri));
        byAlternateIds
                .stream()
                .filter(d -> StringUtils.equals(d.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .forEach(depositId -> {
                    ZenodoContext ctx = new ZenodoContext(this.ctx.getAccessToken(), this.ctx.getEndpoint());
                    ctx.setDepositId(depositId);
                    try {
                        ZenodoUtils.delete(ctx);
                    } catch (IOException e) {
                        // ignore
                    }
                });
    }

    private String getContentId() {
        return "hash://md5/866f169536bbe438dbda3c1683fd69b1";
    }

    @Test
    public void noUpdateOnSameSubmission() throws IOException, ContentStreamException {

        UUID uuid = UUID.randomUUID();

        IRI contentId = ZenodoTestUtil.contentIdFor(uuid);
        String metadata = ZenodoTestUtil.getMetadataSample(uuid, contentId);


        ZenodoMetadataFileStreamHandler handler = new ZenodoMetadataFileStreamHandler(
                new ContentStreamHandler() {
                    @Override
                    public boolean handle(IRI version, InputStream in) throws ContentStreamException {
                        return false;
                    }

                    @Override
                    public boolean shouldKeepProcessing() {
                        return true;
                    }
                },
                ZenodoTestUtil.dereferencerFor(uuid),
                new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {

                    }
                },
                ctx,
                Arrays.asList(RefNodeFactory.toStatement(
                        RefNodeFactory.toIRI("https://example.org/file.txt"),
                        HAS_VERSION,
                        contentId)
                )
        );


        Collection<Pair<Long, String>> idsBefore = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList(contentId.getIRIString()),
                "",
                ResourcesHTTP::asInputStream);


        handler.handle(
                contentId,
                IOUtils.toInputStream(metadata, StandardCharsets.UTF_8)
        );

        Collection<Pair<Long, String>> idsAfterFirst = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList(contentId.getIRIString()),
                "",
                ResourcesHTTP::asInputStream);
        assertThat(idsAfterFirst, not(nullValue()));
        assertThat(idsAfterFirst.size(), is(idsBefore.size() + 1));

        handler.handle(
                RefNodeFactory.toIRI(getContentId()),
                IOUtils.toInputStream(metadata, StandardCharsets.UTF_8)
        );

        Collection<Pair<Long, String>> idsAfterSecond = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList(contentId.getIRIString()),
                "",
                ResourcesHTTP::asInputStream);
        assertThat(idsAfterSecond, not(nullValue()));
        assertThat(idsAfterSecond.size(), is(idsAfterFirst.size()));

    }


}