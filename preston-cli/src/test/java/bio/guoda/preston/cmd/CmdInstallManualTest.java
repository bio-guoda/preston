package bio.guoda.preston.cmd;

import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

public class CmdInstallManualTest  {

    @Test
    public void listManpageFiles() {
        Collection<String> names = CmdInstallManual.listManpageFilenames();
        assertThat(names, hasItem("preston.1"));
        assertThat(names, hasItem("preston-cat.1"));
    }


}