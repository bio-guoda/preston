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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class ResourcesHTTPIT {

    @Test
    public void irelandServerPickyAboutContentHeaderWithJSON() throws IOException {
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip")))) {
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
        //System.setProperty("ZENODO_TOKEN", "[insert token here]");
        try (InputStream is
                     = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(URI.create("https://sandbox.zenodo.org/api/deposit/depositions")))) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copy(is, outputStream);
            JsonNode jsonNode = new ObjectMapper().readTree(outputStream.toByteArray());

            assertThat(jsonNode.isArray(), Is.is(true));
        }

    }

}