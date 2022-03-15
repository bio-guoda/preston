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
    public void citeBiodiversityDataset() throws IOException {
        // see https://github.com/bio-guoda/preston/issues/42
        CSLItemData item = new CSLItemDataBuilder()
                .type(CSLType.DATASET)
                .title("A Biodiversity Dataset. hash://sha256/d79fb9207329a2813b60713cf0968fda10721d576dcb7a36038faf18027eebc1")
                .build();

        String bibl = CSL
                .makeAdhocBibliography(
                        "/bio/guoda/preston/process/ieee-test",
                        "text",
                        item)
                .makeString();

        assertThat(bibl, Is.is("[1]“A Biodiversity Dataset. hash://sha256/d79fb9207329a2813b60713cf0968fda10721d576dcb7a36038faf18027eebc1.” .\n"));
    }

    @Test
    public void citeDWCHash() throws IOException {
        // see https://github.com/bio-guoda/preston/issues/42
        CSLItemData item = new CSLItemDataBuilder()
                .type(CSLType.DATASET)
                .title("A Biodiversity Dataset. hash://sha256/d79fb9207329a2813b60713cf0968fda10721d576dcb7a36038faf18027eebc1")
                .build();

        String bibl = CSL
                .makeAdhocBibliography(
                        "/bio/guoda/preston/process/ieee-test",
                        "text",
                        item)
                .makeString();

        assertThat(bibl, Is.is("[1]“A Biodiversity Dataset. hash://sha256/d79fb9207329a2813b60713cf0968fda10721d576dcb7a36038faf18027eebc1.” .\n"));
    }
}
