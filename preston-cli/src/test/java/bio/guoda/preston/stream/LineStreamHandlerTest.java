package bio.guoda.preston.stream;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class LineStreamHandlerTest {

    @Test
    public void handle() throws IOException, ContentStreamException {
        List<String> lines = handle("/bio/guoda/preston/process/bhl_item_no_BOM.txt");

        assertThat(lines.size(), is(11));
        assertThat(lines.get(0), is("ItemID\tTitleID\tThumbnailPageID\tBarCode\tMARCItemID\tCallNumber\tVolumeInfo\tItemURL\tLocalID\tYear\tInstitutionName\tZQuery\tCreationDate"));
        assertThat(lines.get(lines.size() - 3), is("935\t64\t36774\tmobot31753002306857\ti11595206\tQK1 .F418\tv.23:no.2 (1840)\thttps://www.biodiversitylibrary.org/item/935 \t\t1840\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00"));
        assertThat(lines.get(lines.size() - 1), is(""));
    }

    @Ignore("see Byte Order Mark issue at https://github.com/bio-guoda/preston/issues/122")
    @Test
    public void handleWithBOM() throws IOException, ContentStreamException {
        List<String> lines = handle("/bio/guoda/preston/process/bhl_item.txt");

        assertThat(lines.size(), is(11));
        assertThat(lines.get(0), is("ItemID\tTitleID\tThumbnailPageID\tBarCode\tMARCItemID\tCallNumber\tVolumeInfo\tItemURL\tLocalID\tYear\tInstitutionName\tZQuery\tCreationDate"));
        assertThat(lines.get(lines.size() - 3), is("935\t64\t36774\tmobot31753002306857\ti11595206\tQK1 .F418\tv.23:no.2 (1840)\thttps://www.biodiversitylibrary.org/item/935 \t\t1840\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00"));
        assertThat(lines.get(lines.size() - 1), is(""));
    }

    private List<String> handle(String pathToResource) throws ContentStreamException, IOException {
        List<String> lines = new LinkedList<>();
        ContentStreamHandler testHandler = new ContentStreamHandler() {
            @Override
            public boolean handle(IRI version, InputStream in) {
                try {
                    lines.add(IOUtils.toString(in, StandardCharsets.UTF_8.name()));
                } catch (IOException e) {
                    // Oh no!
                }
                return true;
            }

            @Override
            public boolean shouldKeepReading() {
                return true;
            }
        };

        IRI contentIri = RefNodeFactory.toIRI("blah");
        BlobStoreReadOnly store = getTestBlobStoreForResource(pathToResource);

        LineStreamHandler lineHandler = new LineStreamHandler(testHandler);
        lineHandler.handle(contentIri, store.get(contentIri));

        return lines;
    }



}