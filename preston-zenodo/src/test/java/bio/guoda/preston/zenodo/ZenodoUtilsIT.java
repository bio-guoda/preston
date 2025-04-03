package bio.guoda.preston.zenodo;

import org.junit.Test;

import java.io.IOException;

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


}