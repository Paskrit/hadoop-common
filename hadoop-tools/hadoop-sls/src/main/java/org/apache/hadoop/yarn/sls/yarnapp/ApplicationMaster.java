package org.apache.hadoop.yarn.sls.yarnapp;


import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster {

    public class RMCallbackHandler implements CallbackHandler {

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            LOG.info(
                    "Got response from RM for container ask, completed	count = {}",
                    statuses.size());

            for (ContainerStatus s : statuses) {
                numCompletedContainers.incrementAndGet();
                LOG.info("Container completed : {} with status = {}",
                        s.getContainerId(), s.getState().toString());
            }
        }

        @Override
        public void onContainersAllocated(List<Container> containers) {
            LOG.info(
                    "Got response from RM for container ask, allocated	count = {}",
                    containers.size());

            for (Container container : containers) {
                LOG.info("Starting Container on {}",
                        container.getNodeHttpAddress());

                ContainerLauncher c = new ContainerLauncher(container,
                        containerListener);
                Thread t = new Thread(c);
                t.start();
                launchThreads.add(t);
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            // Leave empty
        }

        @Override
        public float getProgress() {
            float progress = numOfContainers <= 0 ? 0
                    : (float) numCompletedContainers.get() / numOfContainers;
            return progress;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            amRMClient.stop();
        }

    }

    protected class ContainerLauncher implements Runnable {

        private Container container;
        private NMCallbackHandler containerListener;

        public ContainerLauncher(Container container,
                                 NMCallbackHandler containerListener) {
            super();
            this.container = container;
            this.containerListener = containerListener;
        }

        public String getLaunchCommand(Container container) throws IOException {
            Vector<CharSequence> vargs = new Vector<>(30);
            vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
            vargs.add("org.apache.hadoop.yarn.sls.yarnapp.MyContainer ");
            vargs.add(inputFile.toString()); // File to read
            vargs.add(outputPath);
            vargs.add("1><LOG_DIR>/MyContainer.stdout");
            vargs.add("2><LOG_DIR>/MyContainer.stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            return command.toString();
        }

        @Override
        public void run() {
            LOG.info("Setting up ContainerLauncher for containerid = {}",
                    container.getId());

            Map<String, LocalResource> localResources = new HashMap<>();
            Map<String, String> env = System.getenv();

            LocalResource appJarFile = Records.newRecord(LocalResource.class);
            appJarFile.setType(LocalResourceType.FILE);
            appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
            try {
                appJarFile.setResource(ConverterUtils
                        .getYarnUrlFromURI(new URI(env.get("AMJAR"))));
            } catch (URISyntaxException e) {
                e.printStackTrace();
                return;
            }
            appJarFile.setTimestamp(Long.valueOf((env.get("AMJARTIMESTAMP"))));
            appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
            localResources.put("app.jar", appJarFile);
            LOG.info("Added {} as a local resource to the Container",
                    appJarFile.toString());

            ContainerLaunchContext clc = Records
                    .newRecord(ContainerLaunchContext.class);
            clc.setEnvironment(env);
            clc.setLocalResources(localResources);

            try {
                String command = getLaunchCommand(container);
                List<String> commands = new ArrayList<>();
                commands.add(command);
                LOG.info("Command to execute Container = {}", command);

                clc.setCommands(commands);

                //Set up tokens for containers
                clc.setTokens(allTokens.duplicate());
                nmClient.startContainerAsync(container, clc);

                LOG.info("Container {} launched!", container.getId());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(ApplicationMaster.class);
    private YarnConfiguration conf;
    private AMRMClientAsync<ContainerRequest> amRMClient;
    private FileSystem fileSystem;
    private int numOfContainers;
    protected AtomicInteger numCompletedContainers = new AtomicInteger();
    private volatile boolean done;
    // handle NM communication
    protected NMClientAsync nmClient;
    private NMCallbackHandler containerListener;
    // follow thread launched
    private List<Thread> launchThreads = new ArrayList<>();
    // input file argument
    private Path inputFile;
    private String outputPath;
    private ByteBuffer allTokens;

    public ApplicationMaster(String[] args) throws IOException {
        conf = new YarnConfiguration();
        fileSystem = FileSystem.get(conf);
        inputFile = new Path(args[0]);
        outputPath = args[1];
    }

    public void run() throws YarnException, IOException {

        LOG.info("Removing outputFolder if present: {}", outputPath);
        fileSystem.delete(new Path(outputPath), true);

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token.toString());
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000,
                new RMCallbackHandler());
        amRMClient.init(conf);
        amRMClient.start();

        // Register with RM
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(NetUtils.getHostname(), -1, "");

        LOG.info("ApplicationMaster is registered with response: {}",
                response.toString());

        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("AM: Max mem capabililty of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("AM: Max vcores capabililty of resources in this cluster " + maxVCores);

        // Define container handler
        containerListener = new NMCallbackHandler(this);
        nmClient = NMClientAsync.createNMClientAsync(containerListener);
        nmClient.init(conf);
        nmClient.start();

        Resource capacity = Records.newRecord(Resource.class);
        capacity.setMemory(4096);
        capacity.setVirtualCores(4);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        BlockLocation[] blocks = this.getBlockLocations();
        Set<String> distinctHosts = new HashSet<String>();
        for (BlockLocation block : blocks) {
            String[] hosts = block.getHosts();
            for (String host : hosts) {
                distinctHosts.add(host);
            }
        }
        ContainerRequest ask = new ContainerRequest(capacity, distinctHosts.toArray(new String[distinctHosts.size()]), null, priority, false);
        numOfContainers++;
        amRMClient.addContainerRequest(ask);
        LOG.info("Asking for Container for Hosts {}", distinctHosts.toString());

        // Wait for containers to be done
        while (!done && (numCompletedContainers.get() < numOfContainers)) {
            LOG.info("The number of completed Containers = "
                    + this.numCompletedContainers.get());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // Join all launched threads: needed for when we time
        // out and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);

            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: {}", e.getMessage());
                e.printStackTrace();
            }
        }

        LOG.info("Containers have all completed, so shutting down NMClient and AMRMClient...");

        // stop NM handler
        nmClient.stop();
        // Unregister with ResourceManager
        amRMClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "Application complete !",
                null);
        amRMClient.stop();

        //TODO delete the HDFS jar file used by the Application Master
    }

    public BlockLocation[] getBlockLocations() throws IOException {
        // Read the block information from HDFS
        FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
        LOG.info("File status = {}", fileStatus.toString());
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,
                0, fileStatus.getLen());
        LOG.info("Number of blocks for {} = {}", inputFile.toString(),
                blocks.length);
        return blocks;
    }

    public static void main(String[] args) {
        LOG.info("Starting ApplicationMaster...");
        try {
            ApplicationMaster master = new ApplicationMaster(args);
            master.run();
        } catch (IOException | YarnException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
