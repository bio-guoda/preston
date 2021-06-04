package bio.guoda.preston.stream;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.TextMatcher;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.tika.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class LineStreamHandlerTest {

    @Test
    public void handle() throws IOException, ContentStreamException {
        List<String> lines = new LinkedList<>();
        ContentStreamHandler testHandler = new ContentStreamHandler() {
            @Override
            public boolean handle(IRI version, InputStream in) {
                try {
                    lines.add(IOUtils.toString(new InputStreamReader(in)));
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
        BlobStoreReadOnly store = getTestBlobStoreForResource("/bio/guoda/preston/process/bhl_item.txt");

        LineStreamHandler lineHandler = new LineStreamHandler(testHandler);
        lineHandler.handle(contentIri, store.get(contentIri));

        assertThat(lines.size(), is(11));
        assertThat(lines.get(0), is("\uFEFFItemID\tTitleID\tThumbnailPageID\tBarCode\tMARCItemID\tCallNumber\tVolumeInfo\tItemURL\tLocalID\tYear\tInstitutionName\tZQuery\tCreationDate"));
        assertThat(lines.get(lines.size() - 3), is("935\t64\t36774\tmobot31753002306857\ti11595206\tQK1 .F418\tv.23:no.2 (1840)\thttps://www.biodiversitylibrary.org/item/935 \t\t1840\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00"));
        assertThat(lines.get(lines.size() - 1), is(""));
    }

    @Test
    public void passLinesToTextMatcher() throws IOException, ContentStreamException {
        List<Quad> nodes = new LinkedList<>();
        ContentStreamHandler handler = new ContentStreamHandler() {
            final ContentStreamHandler handler = new ContentStreamHandlerImpl(
                    new LineStreamHandler(this),
                    new MatchingTextStreamHandler(this, nodes::add, TextMatcher.URL_PATTERN, new AtomicInteger())
            );

            @Override
            public boolean handle(IRI version, InputStream in) throws ContentStreamException {
                return handler.handle(version, in);
            }

            @Override
            public boolean shouldKeepReading() {
                return true;
            }
        };

        IRI contentIri = RefNodeFactory.toIRI("blah");
        BlobStoreReadOnly store = getTestBlobStoreForResource("/bio/guoda/preston/process/bhl_item.txt");

        handler.handle(contentIri, store.get(contentIri));

        assertThat(nodes.size(), is(9));
        assertThat(nodes.get(0).toString(), is("<cut:line:blah!/L2!/b59-101> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\" ."));
        assertThat(nodes.get(nodes.size() - 1).toString(), is("<cut:line:blah!/L10!/b66-109> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\" ."));
    }

}