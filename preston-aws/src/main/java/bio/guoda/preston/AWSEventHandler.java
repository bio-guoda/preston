package bio.guoda.preston;

import bio.guoda.preston.cmd.CmdGet;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class AWSEventHandler implements RequestStreamHandler {

    private Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        LambdaLogger logger = context.getLogger();

        RequestData requestData = gson.fromJson(IOUtils.toString(inputStream, StandardCharsets.UTF_8), RequestData.class);
        logger.log("Received request with body " + gson.toJson(requestData));

        CmdGet get = new CmdGet();
        get.setDisableCache(true);
        get.setInputStream(IOUtils.toInputStream(requestData.id, StandardCharsets.UTF_8));
        get.setRemotes(Collections.singletonList(requestData.remote));
        get.setOutputStream(outputStream);
        get.run();
    }
}
