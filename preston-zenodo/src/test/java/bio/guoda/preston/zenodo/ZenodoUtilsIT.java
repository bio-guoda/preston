package bio.guoda.preston.zenodo;

import bio.guoda.preston.ResourcesHTTP;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;

public class ZenodoUtilsIT {

    @Test
    public void createEmpty() throws IOException {

        ZenodoContext emptyDepositContext = null;

        try {
            ZenodoContext ctx = ZenodoTestUtil.createSandboxContext();
            assertThat(ctx.getDepositId(), is(nullValue()));
            emptyDepositContext = ZenodoUtils.createEmptyDeposit(ctx);
            assertThat(emptyDepositContext.getDepositId(), is(greaterThan(0L)));
        } finally {
            if (emptyDepositContext != null) {
                ZenodoUtils.delete(emptyDepositContext);
                assertThat(emptyDepositContext.getDepositId(), is(greaterThan(0L)));
            }
        }

    }

    @Test(expected = IOException.class)
    public void createEmptyNoCredentials() throws IOException {
        ZenodoContext ctx = new ZenodoContext(null, "https://sandbox.zenodo.org");
        assertThat(ctx.getDepositId(), is(nullValue()));
        ZenodoUtils.createEmptyDeposit(ctx);
    }

    @Test
    public void findNonExistingWithMatchingNamespacePrefix() throws IOException {

        ZenodoContext ctx = new ZenodoContext(null, "https://zenodo.org");
        Collection<Pair<Long, String>> foundRecords = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList("urn:lsid:globalbioticinteractions.org:dataset:globalbioticinteractions/us"),
                null,
                ResourcesHTTP::asInputStream
        );

        assertThat(foundRecords.size(), is(0));
    }

    @Test
    public void findExistingByNamespace() throws IOException {

        ZenodoContext ctx = new ZenodoContext(null, "https://zenodo.org");
        Collection<Pair<Long, String>> foundRecords = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList("urn:lsid:globalbioticinteractions.org:dataset:globalbioticinteractions/template-dataset"),
                null,
                ResourcesHTTP::asInputStream
        );

        assertThat(foundRecords.size(), is(1));
    }

    @Test
    @Test
    public void findExistingByNamespaceInCommunity() throws IOException {

        ZenodoContext ctx = new ZenodoContext(null, "https://zenodo.org", Arrays.asList("globi-review"));
        Collection<Pair<Long, String>> foundRecords = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList("urn:lsid:globalbioticinteractions.org:dataset:globalbioticinteractions/template-dataset"),
                null,
                ResourcesHTTP::asInputStream
        );

        assertThat(foundRecords.size(), is(1));
    }

    @Test
    public void findExistingByNamespaceInNonExistingCommunity() throws IOException {

        ZenodoContext ctx = new ZenodoContext(null, "https://zenodo.org", Arrays.asList("glozi-review"));
        Collection<Pair<Long, String>> foundRecords = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList("urn:lsid:globalbioticinteractions.org:dataset:globalbioticinteractions/template-dataset"),
                null,
                ResourcesHTTP::asInputStream
        );

        assertThat(foundRecords.size(), is(0));
    }

    @Test
    public void findExistingByNamespaceInNonExistingCommunities() throws IOException {
        ZenodoContext ctx = new ZenodoContext(null,
                "https://zenodo.org",
                Arrays.asList("glozi-review", "glozi-zeview")
        );

        Collection<Pair<Long, String>> foundRecords = ZenodoUtils.findRecordsByAlternateIds(
                ctx,
                Arrays.asList("urn:lsid:globalbioticinteractions.org:dataset:globalbioticinteractions/template-dataset"),
                null,
                ResourcesHTTP::asInputStream
        );

        assertThat(foundRecords.size(), is(0));
    }


}