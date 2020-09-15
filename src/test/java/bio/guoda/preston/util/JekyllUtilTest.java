package bio.guoda.preston.util;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.tika.io.IOUtils;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;

public class JekyllUtilTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void generateRecordSetPage() throws IOException {
        final String resource = "/bio/guoda/preston/process/idigbio-recordsets-complete.json";
        Map<UUID, ByteArrayOutputStream> osMap = writePages(resource, JekyllUtil.RecordType.recordset);
        assertThat(osMap.size(), Is.is(100));

        final String actual = osMap.get(UUID.fromString("d2e46893-099f-45eb-9a76-d2a66f43bec8")).toString("UTF-8");
        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("/bio/guoda/preston/process/jekyll/recordset.md"), StandardCharsets.UTF_8.name())));
    }

    @Test
    public void generateMediaPages() throws IOException {
        final String resource = "/bio/guoda/preston/process/idigbio-mediarecord.json";

        Map<UUID, ByteArrayOutputStream> osMap = writePages(resource, JekyllUtil.RecordType.mediarecord);

        assertThat(osMap.size(), Is.is(1));

        final String actual = osMap.get(UUID.fromString("45e8135c-5cd9-4424-ae6e-a5910d3f2bb4")).toString("UTF-8");
        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("/bio/guoda/preston/process/jekyll/mediarecord.md"), StandardCharsets.UTF_8.name())));
    }

    @Test
    public void generateRecordPage() throws IOException {
        final String resource = "/bio/guoda/preston/process/idigbio-records-complete.json";

        Map<UUID, ByteArrayOutputStream> osMap = writePages(resource, JekyllUtil.RecordType.record);

        assertThat(osMap.size(), Is.is(2));

        final String actual = osMap.get(UUID.fromString("e6c5dffc-4ad1-4d9d-800f-5796baec1f65")).toString("UTF-8");
        assertThat(actual, Is.is(IOUtils.toString(getClass().getResourceAsStream("/bio/guoda/preston/process/jekyll/record.md"), StandardCharsets.UTF_8.name())));
    }

    public Map<UUID, ByteArrayOutputStream> writePages(String resource, JekyllUtil.RecordType recordType) throws IOException {
        final InputStream is = getClass().getResourceAsStream(resource);
        Map<UUID, ByteArrayOutputStream> osMap = new HashMap<>();
        final JekyllUtil.JekyllPageFactory pageFactory = uuid -> {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            osMap.put(uuid, os);
            return os;
        };
        JekyllUtil.writePages(is, pageFactory, recordType);
        return osMap;
    }

    @Test
    public void generateJekyllContent() throws IOException {

        final IRI recordsetIRI = toIRI(URI.create("https://search.idigbio.org/v2/search/recordsets"));
        final IRI recordsetViewIRI = toIRI(URI.create("https://search.idigbio.org/v2/view/recordsets/blabla"));
        final IRI mediarecordIRI = toIRI(URI.create("https://search.idigbio.org/v2/view/mediarecords/blabla"));
        final IRI recordIRI = toIRI(URI.create("https://search.idigbio.org/v2/search/records"));

        Map<IRI, String> resourceMap = new HashMap<IRI, String>() {{
            put(toIRI("recordset:search"), "bio/guoda/preston/process/idigbio-recordsets-complete.json");
            put(toIRI("recordset:view"), "bio/guoda/preston/process/idigbio-recordset.json");
            put(toIRI("mediarecord:view"), "bio/guoda/preston/process/idigbio-mediarecord.json");
            put(toIRI("record:search"), "bio/guoda/preston/process/idigbio-records-complete.json");
        }};

        BlobStoreReadOnly store = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                final String s = resourceMap.get(key);
                return StringUtils.isBlank(s)
                        ? null
                        : getClass().getResourceAsStream("/" + s);
            }
        };

        final File jekyllDir = folder.getRoot();

        final StatementListener statementListener = JekyllUtil.createJekyllSiteGenerator(store, jekyllDir);

        statementListener.on(toStatement(recordIRI, HAS_VERSION, toIRI("record:search")));
        statementListener.on(toStatement(recordsetIRI, HAS_VERSION, toIRI("recordset:search")));
        statementListener.on(toStatement(recordsetViewIRI, HAS_VERSION, toIRI("recordset:view")));
        statementListener.on(toStatement(mediarecordIRI, HAS_VERSION, toIRI("mediarecord:view")));

        final File expectedLayoutDir = new File(jekyllDir, "_layouts");
        assertThat(expectedLayoutDir.exists(), Is.is(true));
        assertThat(expectedLayoutDir.isDirectory(), Is.is(true));
        assertThat(new File(expectedLayoutDir, "mediarecord.html").exists(), Is.is(true));
        assertThat(new File(expectedLayoutDir, "record.html").exists(), Is.is(true));
        assertThat(new File(expectedLayoutDir, "recordset.html").exists(), Is.is(true));

        checkPageUUID(jekyllDir, "d2e46893-099f-45eb-9a76-d2a66f43bec8.md");
        checkPageUUID(jekyllDir, "ba77d411-4179-4dbd-b6c1-39b8a71ae795.md");
        checkPageUUID(jekyllDir, "45e8135c-5cd9-4424-ae6e-a5910d3f2bb4.md");
        checkPageUUID(jekyllDir, "e6c5dffc-4ad1-4d9d-800f-5796baec1f65.md");

        final File urlContentVersion = new File(new File(jekyllDir, "_data"), "content.tsv");
        assertThat(urlContentVersion.exists(), Is.is(true));
        assertThat(urlContentVersion.isFile(), Is.is(true));

    }

    @Test
    public void lastKnownCrawlDate() {
        final Quad activity = toStatement(toIRI("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"), RefNodeConstants.WAS_STARTED_BY, toIRI("https://preston.guoda.bio"));
        final Quad startTime = toStatement(toIRI("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"), RefNodeConstants.STARTED_AT_TIME, toLiteral("2020-09-12T05:54:48.034Z", toIRI("http://www.w3.org/2001/XMLSchema#dateTime")));

        AtomicReference<DateTime> lastPrestonActivityStartTime = new AtomicReference<>();
        final StatementsListener statementsListenerAdapter
                = JekyllUtil.createPrestonStartTimeListener(lastPrestonActivityStartTime::set);

        statementsListenerAdapter.on(activity);
        assertThat(lastPrestonActivityStartTime.get(), Is.is(nullValue()));
        statementsListenerAdapter.on(startTime);
        assertThat(lastPrestonActivityStartTime.get(), Is.is(Matchers.not(nullValue())));
        assertThat(lastPrestonActivityStartTime.get().toString(), Is.is("2020-09-12T05:54:48.034Z"));
    }

    @Test
    public void lastKnownCrawlDateEventFirst() {
        final Quad activity = toStatement(toIRI("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"), RefNodeConstants.WAS_STARTED_BY, toIRI("https://preston.guoda.bio"));
        final Quad startTime = toStatement(toIRI("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"), RefNodeConstants.STARTED_AT_TIME, toLiteral("2020-09-12T05:54:48.034Z", toIRI("http://www.w3.org/2001/XMLSchema#dateTime")));

        AtomicReference<DateTime> lastPrestonActivityStartTime = new AtomicReference<>();
        final StatementsListener statementsListenerAdapter
                = JekyllUtil.createPrestonStartTimeListener(lastPrestonActivityStartTime::set);

        statementsListenerAdapter.on(startTime);
        assertThat(lastPrestonActivityStartTime.get(), Is.is(nullValue()));
        statementsListenerAdapter.on(activity);
        assertThat(lastPrestonActivityStartTime.get(), Is.is(Matchers.not(nullValue())));
        assertThat(lastPrestonActivityStartTime.get().toString(), Is.is("2020-09-12T05:54:48.034Z"));
    }

    @Test
    public void noLastKnownCrawlDateEventFirst() {
        final Quad startTime = toStatement(toIRI("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"), RefNodeConstants.STARTED_AT_TIME, toLiteral("2020-09-12T05:54:48.034Z", toIRI("http://www.w3.org/2001/XMLSchema#dateTime")));

        AtomicReference<DateTime> lastPrestonActivityStartTime = new AtomicReference<>();
        final StatementsListener statementsListenerAdapter
                = JekyllUtil.createPrestonStartTimeListener(lastPrestonActivityStartTime::set);

        statementsListenerAdapter.on(startTime);
        statementsListenerAdapter.on(startTime);
        statementsListenerAdapter.on(startTime);
        statementsListenerAdapter.on(startTime);
        assertThat(lastPrestonActivityStartTime.get(), Is.is(nullValue()));
    }



    private void checkPageUUID(File jekyllDir, String child) {
        final File subDir = new File(new File(jekyllDir, "pages"), child.substring(0, 2));
        final File file = new File(subDir, child);
        assertThat("missing [" + child + "] at [" + file.getAbsolutePath() + "]", file.exists(), Is.is(true));
    }


}