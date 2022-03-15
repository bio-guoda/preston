package bio.guoda.preston.util;

import de.undercouch.citeproc.CSL;
import de.undercouch.citeproc.csl.CSLItemData;
import de.undercouch.citeproc.csl.CSLItemDataBuilder;
import de.undercouch.citeproc.csl.CSLType;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;

public class CitationUtilTest {

    @Test
    public void citePreston() throws IOException {
        CSLItemData item = new CSLItemDataBuilder()
                .type(CSLType.WEBPAGE)
                .title("citeproc-java: A Citation Style Language (CSL) processor for Java")
                .author("Michel", "Krämer")
                .issued(2020, 11, 8)
                .URL("http://michel-kraemer.github.io/citeproc-java/")
                .accessed(2022, 3, 15)
                .build();

        String bibl = CSL
                .makeAdhocBibliography(
                        "/bio/guoda/preston/process/ieee-test",
                        "text",
                        item)
                .makeString();

        assertThat(bibl, Is.is("[1]M. Krämer, “citeproc-java: A Citation Style Language (CSL) processor for Java,” Nov. 08, 2020. http://michel-kraemer.github.io/citeproc-java/ (accessed Mar. 15, 2022).\n"));

    }
}
