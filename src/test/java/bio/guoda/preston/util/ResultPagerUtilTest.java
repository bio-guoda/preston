package bio.guoda.preston.util;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementEmitter;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ResultPagerUtilTest {

    @Test
    public void offsetOrDefault() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records/?rq=%7B%22occurrenceid%22%3A+%22B4298E10-B8F0-4221-8C03-79B7394D40AF%22%7D&offset=5&limit=2");

        final Long offset = ResultPagerUtil.queryValueParamOrDefault(aPage, "offset", 10000L);

        assertThat(offset, is(5L));
    }

    @Test
    public void offsetDefault() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&limit=2");
        final Long offset = ResultPagerUtil.queryValueParamOrDefault(aPage, "offset", 10000L);
        assertThat(offset, is(10000L));
    }


    @Test
    public void nextRecordPageProvidedLimitNoOffset() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&limit=2");

        final IRI iri = nextQueryPageRequest(aPage);

        final String prefix = "https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&";
        assertThat(iri.getIRIString(), is(prefix + "limit=2&offset=2"));


    }

    @Test
    public void nextRecordPageProvidedDefaultLimitNoOffset() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D");

        final IRI iri = nextQueryPageRequest(aPage);

        final String prefix = "https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&";
        assertThat(iri.getIRIString(), is(prefix + "limit=10000&offset=10000"));
    }

    @Test
    public void nextRecordPageProvidedLimitWithOffset() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&limit=2&offset=16");

        final IRI iri = nextQueryPageRequest(aPage);

        final String prefix = "https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&";
        assertThat(iri.getIRIString(), is(prefix + "limit=2&offset=18"));


    }

    @Test
    public void nextRecordPageProvidedDefaultLimitWithOffset() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&" + "offset=12");

        final IRI iri = nextQueryPageRequest(aPage);

        final String prefix = "https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22a6eee223-cf3b-4079-8bb2-b77dad8cae9d%22%7D&";
        assertThat(iri.getIRIString(), is(prefix + "limit=10000&offset=10012"));
    }

    @Test
    public void nextRecordPageProvidedDefaultLimitOnly() {
        final IRI aPage = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/records" + "?limit=12");

        final IRI iri = nextQueryPageRequest(aPage);

        final String prefix = "https://search.idigbio.org/v2/search/records";
        assertThat(iri.getIRIString(), is(prefix + "?limit=12&offset=12"));
    }

    @Test
    public void requestRemainingPages() {
        final String prefix = "https://search.idigbio.org/v2/search/records/?foo=bar";
        final IRI aPage = RefNodeFactory.toIRI(prefix);

        List<BlankNodeOrIRI> pages = new ArrayList<>();

        final Long totalRecords = 10L;
        final Long recordsFound = 2L;
        final StatementEmitter emitter = new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                pages.add(statement.getSubject());
            }
        };

        ResultPagerUtil.emitPageRequests(aPage, totalRecords, recordsFound, emitter);

        assertThat(pages.size(), is(5));
        assertThat(pages.get(0).ntriplesString(), is("<" + prefix + "&limit=2&offset=2>"));
        assertThat(pages.get(4).ntriplesString(), is("<" + prefix + "&limit=2&offset=10>"));

    }

    @Test
    public void requestRemainingPagesWithNonDefaultLimit() {
        final String prefix = "https://search.idigbio.org/v2/search/records?bla";
        final IRI aPage = RefNodeFactory.toIRI(prefix + "&limit=2");

        List<BlankNodeOrIRI> pages = new ArrayList<>();

        final Long totalRecords = 10L;
        final Long recordsFound = 2L;
        final StatementEmitter emitter = new StatementEmitter() {
            @Override
            public void emit(Quad statement) {
                pages.add(statement.getSubject());
            }
        };

        ResultPagerUtil.emitPageRequests(aPage, totalRecords, recordsFound, emitter);

        assertThat(pages.size(), is(5));
        assertThat(pages.get(0).ntriplesString(), is("<" + prefix + "&limit=2&offset=2>"));
        assertThat(pages.get(1).ntriplesString(), is("<" + prefix + "&limit=2&offset=4>"));
        assertThat(pages.get(2).ntriplesString(), is("<" + prefix + "&limit=2&offset=6>"));
        assertThat(pages.get(3).ntriplesString(), is("<" + prefix + "&limit=2&offset=8>"));
        assertThat(pages.get(4).ntriplesString(), is("<" + prefix + "&limit=2&offset=10>"));
    }


    public IRI nextQueryPageRequest(IRI aPage) {
        final Long offset = ResultPagerUtil.queryValueParamOrDefault(aPage, "offset", 0L);
        final Long limit = ResultPagerUtil.queryValueParamOrDefault(aPage, "limit", 10000L);

        return ResultPagerUtil.queryForPage(aPage, offset + limit, limit);
    }


}