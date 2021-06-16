package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.Test;

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
            IOUtils.copy(is, new NullOutputStream());
        }
    }

    @Test
    public void irelandServerPickyAboutContentHeaderWithJSON404Ignore() throws IOException {
        final AtomicBoolean gotBusyMessage = new AtomicBoolean(false);
        final AtomicBoolean gotDoneMessage = new AtomicBoolean(false);
        try (InputStream is = ResourcesHTTP.asInputStreamIgnore404(RefNodeFactory.toIRI(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip")), new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {
                if (DerefState.BUSY.equals(derefState)) {
                    gotBusyMessage.set(true);
                } else if (DerefState.DONE.equals(derefState)) {
                    gotDoneMessage.set(true);
                }
            }
        })) {
            IOUtils.copy(is, new NullOutputStream());
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
        try (InputStream is = ResourcesHTTP.asInputStreamIgnore404(dataURI, listener)) {
            IOUtils.copy(is, new NullOutputStream());
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

}