package org.apache.hadoop.yarn.sls.yarnapp;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class AppClient {

    private static final Logger LOG = LoggerFactory.getLogger(AppClient.class);
    private static final String APP_NAME = "SLSYarnApp";
    private YarnConfiguration conf;
    private YarnClient yarnClient;
    private String appJar = "hadoop-sls-2.6.0-cdh5.11.0.jar";
    private ApplicationId appId;
    private FileSystem fs;
    private String inputPath;
    private String outputPath;

    public AppClient(String[] args) throws IOException {
        conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        fs = FileSystem.get(conf);
        inputPath = args[0];
        outputPath = args[1];
    }

    public boolean run() throws YarnException, IOException {
        yarnClient.start();
        YarnClientApplication yarnApp = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = yarnApp
                .getNewApplicationResponse();

        // Get app Id
        appId = appResponse.getApplicationId();
        LOG.info("Application ID = {}", appId);

        // Cluster Metrics
        // vCPU and Memory
        int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
        int maxVCores = appResponse.getMaximumResourceCapability()
                .getVirtualCores();
        LOG.info("Max memory = {} and max vcores = {}", maxMemory, maxVCores);
        // number of NM
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        int nNM = clusterMetrics.getNumNodeManagers();
        LOG.info("Number of NM = {}", nNM);

        // Copy jar to hdfs
        Path src = new Path(this.appJar);
        String pathSuffix = APP_NAME + "/" + appId.getId() + "/app.jar";
        Path dest = new Path(fs.getHomeDirectory(), pathSuffix);
        fs.copyFromLocalFile(false, true, src, dest);
        FileStatus destStatus = fs.getFileStatus(dest);

        // Create the LocalResource
        LocalResource jarResource = Records.newRecord(LocalResource.class);
        jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dest));
        jarResource.setTimestamp(destStatus.getModificationTime());
        jarResource.setSize(destStatus.getLen());
        jarResource.setType(LocalResourceType.FILE);
        jarResource.setVisibility(LocalResourceVisibility.APPLICATION);
        Map<String, LocalResource> localResources = new HashMap<>();
        localResources.put("app.jar", jarResource);

        // Env Variable
        Map<String, String> env = new HashMap<>();
        String appJarDest = dest.toUri().toString();
        env.put("AMJAR", appJarDest);
        LOG.info("AMJAR environment variable is set to {}", appJarDest);
        env.put("APPUSER", UserGroupInformation.getCurrentUser()
                .getShortUserName());
        env.put("AMJARTIMESTAMP",
                Long.toString(destStatus.getModificationTime()));
        env.put("AMJARLEN", Long.toString(destStatus.getLen()));

        // Launch Environment
        StringBuilder classPathEnv = new StringBuilder().append(
                File.pathSeparatorChar).append("./app.jar");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(Environment.CLASSPATH.$());
        env.put("CLASSPATH", classPathEnv.toString());

        // Application Submission Context
        ApplicationSubmissionContext appContext = yarnApp
                .getApplicationSubmissionContext();
        appContext.setApplicationName(APP_NAME);
        appContext.setQueue("dev");
        // Context for Application Master
        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);
        amContainer.setLocalResources(localResources);
        amContainer.setEnvironment(env);

        // Command to launch the AM
        Vector<CharSequence> vargs = new Vector<>(30);
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("org.apache.hadoop.yarn.sls.yarnapp.ApplicationMaster");
        vargs.add(inputPath);
        vargs.add(outputPath);
        vargs.add("1><LOG_DIR>/ApplicationMaster.stdout");
        vargs.add("2><LOG_DIR>/ApplicationMaster.stderr");
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        amContainer.setCommands(commands);
        LOG.info("Command to execute ApplicationMaster = {}", command);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1024);
        appContext.setResource(capability);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            final Token<?> tokens[] =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        // CLC
        appContext.setAMContainerSpec(amContainer);
        appId = appContext.getApplicationId();
        yarnClient.submitApplication(appContext);

        return true;
    }

    public static void main(String[] args) {
        AppClient client;
        try {LOG.info("test");
            client = new AppClient(args);
            boolean result = client.run();
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        }
    }

}
