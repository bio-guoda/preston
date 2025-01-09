package bio.guoda.preston;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class VersionUtilTest {

    @Test
    public void getDevVersion() {
        assertThat(VersionUtil.getVersionString(), Is.is("dev"));
    }

}