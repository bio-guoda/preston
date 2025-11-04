package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;

public class ZoteroFileStreamHandlerRISTest {

    @Test
    public void translateToRIS() throws IOException, ContentStreamException {

        Dereferencer<InputStream> deref = new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                if (StringUtils.equals(uri.getIRIString(), "https://api.zotero.org/groups/5435545/items/DP629R8S")) {
                    return getClass().getResourceAsStream("zotero/ZoteroArticle.json");
                } else {
                    throw new IOException("kaboom!");
                }
            }
        };
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        ZoteroFileStreamHandlerRIS zoteroFileStreamHandlerRIS = new ZoteroFileStreamHandlerRIS(
                null,
                boas,
                null,
                deref,
                RefNodeFactory.toIRI("hash://md5/b1946ac92492d2347c6235b4d2611184")
        );

        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("zotero/ZoteroAttachment.json"));

        zoteroFileStreamHandlerRIS.handleZoteroRecord(jsonNode, "bla", new AtomicBoolean(true));

        assertThat(new String(boas.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "TY  - JOUR\r\n" +
                        "AU  - Lytras, Spyros\r\n" +
                        "AU  - Hughes, Joseph\r\n" +
                        "AU  - Martin, Darren\r\n" +
                        "AU  - Swanepoel, Phillip\r\n" +
                        "AU  - de Klerk, Arne\r\n" +
                        "AU  - Lourens, Rentia\r\n" +
                        "AU  - Kosakovsky Pond, Sergei L.\r\n" +
                        "AU  - Xia, Wei\r\n" +
                        "AU  - Jiang, Xiaowei\r\n" +
                        "AU  - Robertson, David L.\r\n" +
                        "PY  - 2022\r\n" +
                        "TI  - Exploring the natural origins of SARS-CoV-2\r\n" +
                        "T2  - Genome Biology and Evolution\r\n" +
                        "VL  - 14\r\n" +
                        "IS  - 2\r\n" +
                        "SP  - 1-14\r\n" +
                        "DO  - 10.1093/gbe/evac018\r\n" +
                        "AB  - Exploring the natural origins of SARS-CoV-2 Spyros Lytras1, Joseph Hughes1, Xiaowei Jiang2, David L Robertson1  1MRC-University of Glasgow Centre for Virus Research (CVR), Glasgow, UK.  2Department of Biological Sciences, Xi’an Jiaotong-Liverpool University (XJTLU), Suzhou, China.  The lack of an identifiable intermediate host species for the proximal animal ancestor of SARS-CoV-2 and the distance (~1500 km) from Wuhan to Yunnan province, where the closest evolutionary related coronaviruses circ...\r\n" +
                        "ER  - \r\n"));

    }

}