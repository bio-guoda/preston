package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import bio.guoda.preston.stream.MatchingTextStreamHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

public class NameExtractorTest {
@Test
    public void onZipBatchSize100() {

    BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
        @Override
        public InputStream get(IRI key) {
            URL resource = getClass().getResource("/bio/guoda/preston/plazidwca.zip");

            IRI iri = toIRI(resource.toExternalForm());

            if (StringUtils.equals("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1", key.getIRIString())) {
                try {
                    return new FileInputStream(new File(URI.create(iri.getIRIString())));
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        }
    };


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));

        NameExtractor nameExtractor = new NameExtractor(new ProcessorStateAlwaysContinue(), blobStore);
        nameExtractor.on(statement);
    }

}
