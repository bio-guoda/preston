package bio.guoda.preston;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestContext implements Context {

    public TestContext() {
    }

    @Override
    public LambdaLogger getLogger() {
        return new LambdaLogger() {
            private final Logger logger = LoggerFactory.getLogger(getClass());

            @Override
            public void log(String s) {
                logger.info(s);
            }

            @Override
            public void log(byte[] bytes) {
                logger.info(new String(bytes));
            }
        };
    }

    @Override
    public String getAwsRequestId() {
        return null;
    }

    @Override
    public String getLogGroupName() {
        return null;
    }

    @Override
    public String getLogStreamName() {
        return null;
    }

    @Override
    public String getFunctionName() {
        return null;
    }

    @Override
    public String getFunctionVersion() {
        return null;
    }

    @Override
    public String getInvokedFunctionArn() {
        return null;
    }

    @Override
    public CognitoIdentity getIdentity() {
        return null;
    }

    @Override
    public ClientContext getClientContext() {
        return null;
    }

    @Override
    public int getRemainingTimeInMillis() {
        return 0;
    }

    @Override
    public int getMemoryLimitInMB() {
        return 0;
    }
}
