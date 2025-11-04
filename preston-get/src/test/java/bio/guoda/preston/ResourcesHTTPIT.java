package bio.guoda.preston;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;

public class ResourcesHTTPIT {

    @Test
    public void irelandServerPickyAboutContentHeaderWithJSON() throws IOException {
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip")))) {
            IOUtils.copy(is, NullOutputStream.INSTANCE);
        }
    }

    @Test
    public void SciELO403SoftRedirect() throws IOException {
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("http://www.scielo.cl/scielo.php?script=sci_pdf&pid=S0718-19572015000100003")))) {
            IOUtils.copy(is, NullOutputStream.INSTANCE);
        }
    }

    @Test
    public void SciELO403SoftRedirectHttps() throws IOException {
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://www.scielo.cl/scielo.php?script=sci_pdf&pid=S0718-19572015000100003")))) {
            IOUtils.copy(is, NullOutputStream.INSTANCE);
        }
    }

    @Test
    public void irelandServerPickyAboutContentHeaderWithJSON404Ignore() throws IOException {
        final AtomicBoolean gotBusyMessage = new AtomicBoolean(false);
        final AtomicBoolean gotDoneMessage = new AtomicBoolean(false);
        try (InputStream is = ResourcesHTTP.asInputStreamIgnore40x50x(RefNodeFactory.toIRI(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip")), new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {
                if (DerefState.BUSY.equals(derefState)) {
                    gotBusyMessage.set(true);
                } else if (DerefState.DONE.equals(derefState)) {
                    gotDoneMessage.set(true);
                }
            }
        })) {
            IOUtils.copy(is, NullOutputStream.INSTANCE);
        }
        assertThat(gotDoneMessage.get(), Is.is(true));
        assertThat(gotBusyMessage.get(), Is.is(true));
    }

    @Test
    public void followRedirect302WithUnsupportedCertificate() throws IOException {
        // see https://github.com/bio-guoda/preston/issues/25
        final AtomicBoolean gotDoneMessage = new AtomicBoolean(false);
        IRI dataURI = RefNodeFactory.toIRI(URI.create("http://ipt.env.duke.edu/archive.do?r=zd_872"));
        DerefProgressListener listener = (dataURI1, derefState, read, total) -> {
            if (DerefState.DONE.equals(derefState)) {
                gotDoneMessage.set(true);
            }
        };
        try (InputStream is = ResourcesHTTP.asInputStreamIgnore40x50x(dataURI, listener)) {
            IOUtils.copy(is, NullOutputStream.INSTANCE);
        }
        assertThat(gotDoneMessage.get(), Is.is(true));
    }


    @Test
    public void dataOneObjectLocationList() throws IOException {
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://cn.dataone.org/cn/v2/resolve/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FJTH_BMW.20160629")))) {
            StringWriter output = new StringWriter();
            IOUtils.copy(is, output, StandardCharsets.UTF_8);
            assertThat(output.getBuffer().toString(), CoreMatchers.containsString("objectLocationList"));
        }
    }

    @Test
    public void hashURINoRewrite() throws IOException {
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://linker.bio/hash://sha256/edde5b2b45961e356f27b81a3aa51584de4761ad9fa678c4b9fa3230808ea356")))) {
            CountingOutputStream outputStream = new CountingOutputStream(NullOutputStream.INSTANCE);
            IOUtils.copy(is, outputStream);
            assertThat(outputStream.getByteCount(), Is.is(233031L));
        }
    }

    @Test
    public void githubAuth() {
        //System.setProperty("GITHUB_TOKEN", "[insert token here]");
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://api.github.com/repos/globalbioticinteractions/elton/issues?per_page=1&state=open")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);
            assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is("bla"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void zoteroAuth() throws IOException {
        //System.setProperty("ZOTERO_TOKEN", "[insert token here]");
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://api.zotero.org/groups/5435545")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);
            JsonNode jsonNode = new ObjectMapper().readTree(outputStream.toByteArray());
            assertThat(jsonNode.at("/id").longValue(), Is.is(5435545L));
        }

    }

    @Test
    public void zenodoAuth() throws IOException {
        // System.setProperty("ZENODO_TOKEN", "[insert token here]");
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://sandbox.zenodo.org/api/deposit/depositions")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);
            JsonNode jsonNode = new ObjectMapper().readTree(outputStream.toByteArray());

            assertThat(jsonNode.isArray(), Is.is(true));
        }

    }

    @Test
    public void googleAuth() throws IOException {
        //System.setProperty("GOOGLE_TOKEN", "[some access token]");
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://docs.google.com/document/u/0/export?id=1hf_rYn6T8QgnyBoiHvMLZphq9h4BNKPBIoWE5uCzz84&tab=t.0&format=md")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);
            assertThat(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), Is.is("this is a secret"));
        }

    }

    @Test
    public void zenodoAuthQueryRestrictedContentByZenodoIndexedContentHash() throws IOException {
        // Zenodo does not support searching restricted content by their Zenodo indexed hash
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://zenodo.org/api/records?q=files.entries.checksum:%22md5:587f269cfa00aa40b7b50243ea8bdab9%22&allversions=1")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);
            JsonNode jsonNode = new ObjectMapper().readTree(outputStream.toByteArray());

            JsonNode hits = jsonNode.get("hits").get("hits");
            assertThat(hits.isArray(), Is.is(true));
            assertThat(hits.size(), Is.is(0));
        }

    }

    @Test
    public void zenodoAuthAccessRestrictedContent() throws IOException {
        //System.setProperty("ZENODO_TOKEN", "[your token here]");
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://zenodo.org/api/records/13477150/files/Eric%20Mo%C3%AFse%20Bakwo%20Fils%20et%20al.%20-%202022%20-%20New%20record%20and%20update%20on%20the%20geographic%20distributi.pdf/content")))) {
            CountingOutputStream outputStream = new CountingOutputStream(NullOutputStream.INSTANCE);
            IOUtils.copy(is, outputStream);

            assertThat(outputStream.getByteCount(), Is.is(1233336L));
        }
    }

    @Test
    public void zenodoAuthGetRestrictedContent() throws IOException {
        //System.setProperty("ZENODO_TOKEN", "[your token here]");
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://zenodo.org/api/records/13477150/files/Eric%20Mo%C3%AFse%20Bakwo%20Fils%20et%20al.%20-%202022%20-%20New%20record%20and%20update%20on%20the%20geographic%20distributi.pdf/content")))) {
            CountingOutputStream outputStream = new CountingOutputStream(NullOutputStream.INSTANCE);
            IOUtils.copy(is, outputStream);

            assertThat(outputStream.getByteCount(), Is.is(1233336L));
        }

    }

    @Test(expected = IOException.class)
    public void zenodoNoAuthGetRestrictedContent() throws IOException {
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://zenodo.org/api/records/13477150/files/Eric%20Mo%C3%AFse%20Bakwo%20Fils%20et%20al.%20-%202022%20-%20New%20record%20and%20update%20on%20the%20geographic%20distributi.pdf/content")))) {
            CountingOutputStream outputStream = new CountingOutputStream(NullOutputStream.INSTANCE);
            IOUtils.copy(is, outputStream);
        }

    }

    @Test(expected = org.apache.http.client.ClientProtocolException.class)
    public void circularRedirect() throws IOException {
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://www.snib.mx/iptconabio/archive.do?r=ecoab-host-plant")))) {
            CountingOutputStream outputStream = new CountingOutputStream(NullOutputStream.INSTANCE);
            IOUtils.copy(is, outputStream);
        }

    }

}