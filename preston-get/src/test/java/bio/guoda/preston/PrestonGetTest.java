package bio.guoda.preston;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class PrestonGetTest {

    @Test
    public void getNonExistingAlias() {
        assertThat(PrestonGet.run(new String[] {"https://example.org"}), Is.is(1));
    }

    @Test
    public void getNonExistingEmbeddedAlias() {
        assertThat(PrestonGet.run(new String[] {"zip:https://example.org/file.zip!/README"}), Is.is(1));
    }

    @Test
    public void getNonExistingHash() {
        assertThat(PrestonGet.run(new String[] {"hash://sha256/779e81635dc0173def5aa66a0f43c3806d481a2bcba79b45194683e8f408c036"}), Is.is(1));
    }

}