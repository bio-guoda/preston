package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.jena.sparql.core.Quad;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class RDFUtilTest {

    @Test
    public void asQuadStream() {
        Stream<Quad> quadStream = RDFUtil.asQuadStream(IOUtils.toInputStream("<subj> <verb> <obj> <graph> .", StandardCharsets.UTF_8));
        List<Quad> collect = quadStream.collect(Collectors.toList());
        assertThat(collect.size(), is(1));
        Quad quad = collect.get(0);
        assertThat(quad.getObject().toString(), is("obj"));
        assertThat(quad.getPredicate().toString(), is("verb"));
        assertThat(quad.getSubject().toString(), is("subj"));
        assertThat(quad.getGraph().toString(), is("graph"));
    }

}