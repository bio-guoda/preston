package bio.guoda.preston;

import com.amazonaws.services.lambda.runtime.Context;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;

public class AWSEventHandlerIT {

    @Ignore("In case something happens to https://deeplinker.bio")
    @Test
    public void handleRequest() throws IOException {
        InputStream in = IOUtils.toInputStream(
                String.format("{ %s, %s }",
                        "\"id\": \"hash://sha256/2c03651ef6a7c6bed0e672ee8fd481fbf02417c3868aca646fb86841ef577559\"",
                        "\"remote\": \"https://deeplinker.bio\""),
                StandardCharsets.UTF_8);

        Context context = new TestContext();

        OutputStream out = new ByteArrayOutputStream();
        AWSEventHandler handler = new AWSEventHandler();
        handler.handleRequest(in, out, context);

        String theOut = out.toString();
        assertThat(theOut, startsWith("<eml:eml xmlns:eml=\"eml://ecoinformatics.org/eml-2.1.1\""));
    }

    @Test
    public void getSomethingInATarGz() throws IOException {
        InputStream in = IOUtils.toInputStream(
                String.format("{ %s, %s }",
                        "\"id\": \"hash://sha256/d61c5f1747b2e95a53028db1958d6993ca2cd1b7d2352b7bc7619cd15752482d\"",
                        "\"remote\": \"https://zenodo.org/record/3849494/files/\""),
                StandardCharsets.UTF_8);

        Context context = new TestContext();

        OutputStream out = new ByteArrayOutputStream();
        AWSEventHandler handler = new AWSEventHandler();
        handler.handleRequest(in, out, context);

        String theOut = out.toString();
        assertThat(theOut, startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"));
    }
}