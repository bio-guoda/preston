package bio.guoda.preston.stream;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.TextMatcher;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForTextResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MatchingTextStreamHandlerTest {

    @Test
    public void findMatchesInLines() throws IOException, ContentStreamException {
        List<Quad> nodes = new LinkedList<>();
        ContentStreamHandler handler = getLineTextMatcher(nodes, true);

        IRI contentIri = RefNodeFactory.toIRI("blah");
        BlobStoreReadOnly store = getTestBlobStoreForTextResource("/bio/guoda/preston/process/bhl_item_no_BOM.txt");

        handler.handle(contentIri, store.get(contentIri));

        assertThat(nodes.size(), is(9));
        assertThat(nodes.get(0).toString(), is("<cut:line:blah!/L2!/b59-101> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\" ."));
        assertThat(nodes.get(nodes.size() - 1).toString(), is("<cut:line:blah!/L10!/b66-109> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\" ."));
    }

    @Test
    public void findLinesWithMatches() throws IOException, ContentStreamException {
        List<Quad> nodes = new LinkedList<>();
        ContentStreamHandler handler = getLineTextMatcher(nodes, false);

        IRI contentIri = RefNodeFactory.toIRI("blah");
        BlobStoreReadOnly store = getTestBlobStoreForTextResource("/bio/guoda/preston/process/bhl_item.txt");

        handler.handle(contentIri, store.get(contentIri));

        assertThat(nodes.size(), is(9));
        assertThat(nodes.get(0).toString(), is("<line:blah!/L2> <http://www.w3.org/ns/prov#value> \"24\t11\t268274\tmobot31753000022803\ti11499722\tQK98 .R6 1789\t\thttps://www.biodiversitylibrary.org/item/24 \t\t1789\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\" ."));
        assertThat(nodes.get(nodes.size() - 1).toString(), is("<line:blah!/L10> <http://www.w3.org/ns/prov#value> \"947\t64\t44519\tmobot31753002306964\ti11595310\tQK1 .F418\tv.33 (1850)\thttps://www.biodiversitylibrary.org/item/947 \t\t1850\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\" ."));
    }

    private ContentStreamHandler getLineTextMatcher(List<Quad> nodes, boolean reportOnlyMatchingText) {
        return new ContentStreamHandler() {
            final StatementsEmitter emitter = new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            };

            final ContentStreamHandler handler = new ContentStreamHandlerImpl(
                    new LineStreamHandler(this),
                    new MatchingTextStreamHandler(this, emitter, TextMatcher.URL_PATTERN, reportOnlyMatchingText)
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
    }

}