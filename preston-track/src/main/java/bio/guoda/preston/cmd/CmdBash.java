package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.DereferencerContentAddressed;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@CommandLine.Command(
        name = "bash",
        description = "runs provided bash script and tracks stdout"
)
public class CmdBash extends CmdActivity {

    @CommandLine.Option(
            names = {"-c"},
            description = "Content id of script to be executed."
    )
    private IRI commandsContentId;

    private boolean streamingCommands = false;


    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {

    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        StatementsListener processor = createActivityProcessor(blobStore, ctx, listeners);

        try {
            if (getCommandsContentId() == null) {
                setCommandsContentId(blobStore.put(getInputStream()));
                streamingCommands = true;
            }

            statementQueue.add(Arrays.asList(
                    toStatement(getCommandsContentId(), RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral("text/x-shellscript")),
                    toStatement(ctx.getActivity(), USED, getCommandsContentId()),
                    toStatement(ctx.getActivity(), HAS_VERSION, toBlank()))
            );

            while (!statementQueue.isEmpty()) {
                processor.on(statementQueue.poll());
            }
        } catch (IOException e) {
            //
        }
    }

    public void setCommandsContentId(IRI commandsContentId) {
        this.commandsContentId = commandsContentId;
    }

    @Override
    String getActivityDescription() {
        return "Executes script and captures stdout";
    }

    private StatementsListener createActivityProcessor(
            BlobStore blobStore,
            ActivityContext ctx,
            StatementsListener[] listeners) {
        return new Archiver(
                new DereferencerContentAddressed(uri -> {
                    InputStream inputStream = blobStore.get(getCommandsContentId());
                    ProcessBuilder bash = new ProcessBuilder(
                            "bash",
                            "-c",
                            IOUtils.toString(inputStream, StandardCharsets.UTF_8)
                    );

                    Process proc = streamingCommands
                            ? bash.start()
                            : bash.redirectInput(ProcessBuilder.Redirect.from(new File("/dev/stdin"))).start();
                    return proc.getInputStream();
                }, blobStore),
                ctx,
                listeners);
    }


    public IRI getCommandsContentId() {
        return commandsContentId;
    }

}
