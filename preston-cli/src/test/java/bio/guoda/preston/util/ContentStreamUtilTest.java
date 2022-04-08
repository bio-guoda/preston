package bio.guoda.preston.util;

import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ContentStreamUtilTest {

    @Test
    public void cutBytes() throws IOException {
        InputStream inBytes = IOUtils.toInputStream("0123456789", StandardCharsets.UTF_8);
        InputStream outBytes = ContentStreamUtil.cutBytes(inBytes, 4, 7);

        assertThat(IOUtils.toString(outBytes, StandardCharsets.UTF_8), is("456"));
    }

    @Test( expected = IOException.class)
    public void cutBadRange() throws IOException {
        InputStream inBytes = IOUtils.toInputStream("0123456789", StandardCharsets.UTF_8);
        ContentStreamUtil.cutBytes(inBytes, 14, 17);
    }
}