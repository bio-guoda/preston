package bio.guoda.preston;

import org.junit.Test;

public class PrestonGetTest {

    @Test(expected = RuntimeException.class)
    public void getNonExistingAlias() {
        PrestonGet.run(new String[] {"https://example.org"});
    }

    @Test(expected = RuntimeException.class)
    public void getNonExistingHash() {
        PrestonGet.run(new String[] {"hash://sha256/779e81635dc0173def5aa66a0f43c3806d481a2bcba79b45194683e8f408c036"});
    }

}