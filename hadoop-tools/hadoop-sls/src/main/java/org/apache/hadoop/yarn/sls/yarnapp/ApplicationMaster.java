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
import org.apache.hadoop.yarn.client.api.YarnClient;
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

                LOG.info("-----");
                //http://a4-5d-36-fc-ef-50.hpc.criteo.preprod:8042/node/containerlogs/container_e358_1500892691690_0132_01_000001/k.jacquemin/ApplicationMaster.stderr/?start=0
                LOG.info("LOG URL http://{}/node/containerlogs/{}/k.jacquemin/" + containerType + ".stderr/?start=0", container.getNodeHttpAddress(), container.getId());
                LOG.info("-----");

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

            if (containerType.equals("MyContainerSLS")) {
                vargs.add("export HADOOP_ROOT=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0\";");
                vargs.add("export PATH=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/bin:$PATH\";");
                vargs.add("export CLASSPATH=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/etc/hadoop:$CLASSPATH\";");
                vargs.add("export HADOOP_CONF_DIR=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/etc/hadoop\";");
                vargs.add("export HADOOP_HDFS_HOME=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0\";");
                vargs.add("export HADOOP_COMMON_HOME=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0\";");
                vargs.add("export HADOOP_YARN_HOME=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0\";");
                vargs.add("export HADOOP_MAPRED_HOME=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0\";");
                vargs.add("export HADOOP_CLASSPATH=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/share/hadoop/mapreduce1/contrib/capacity-scheduler/*.jar:$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/share/hadoop/tools/lib/*:$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/share/hadoop/tools/html\";");
                vargs.add("export YARN_CONF_DIR=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0/etc/hadoop\";");
                vargs.add("export HADOOP_PREFIX=\"$(readlink -f hadoop.tar)/hadoop-2.6.0-cdh5.11.0\";");
            }
            vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
            vargs.add("-verbose:class");
            vargs.add("org.apache.hadoop.yarn.sls.yarnapp." + containerType + " ");
            vargs.add(inputPath); // File to read
            vargs.add(outputPath);
            vargs.add("1><LOG_DIR>/" + containerType + ".stdout");
            vargs.add("2><LOG_DIR>/" + containerType + ".stderr");
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

            // The container for the eventual shell commands needs its own local
            // resources too.
            // In this scenario, if a shell script is specified, we need to have it
            // copied and made available to the container.
            String tarLocation = env.get("TARLOCATION");
            long tarLocationTimestamp = Long.parseLong(env
                    .get("TARTIMESTAMP"));
            long tarLocationLen = Long.parseLong(env
                    .get("TARLEN"));

            Path tarPath = new Path(tarLocation);
            LocalResource tarRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(tarPath.toUri()),
                    LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
                    tarLocationLen, tarLocationTimestamp);
            localResources.put("hadoop.tar", tarRsrc);
            LOG.info("Added {} as a local resource to the Container",
                    tarRsrc.toString());

            String xmlLocation = env.get("XMLLOCATION");
            long xmlLocationTimestamp = Long.parseLong(env
                    .get("XMLTIMESTAMP"));
            long xmlLocationLen = Long.parseLong(env
                    .get("XMLLEN"));

            Path xmlPath = new Path(xmlLocation);
            LocalResource xmlRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(xmlPath.toUri()),
                    LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                    xmlLocationLen, xmlLocationTimestamp);
            localResources.put("custom-site.xml", xmlRsrc);
            LOG.info("Added {} as a local resource to the Container",
                    xmlRsrc.toString());

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
                LOG.error(e.getLocalizedMessage());
            }
        }
    }

    //
    //ApplicationMaster Attributes
    //

    private YarnClient yarnClient;

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

    private final String inputPath;
    private final String outputPath;
    private ByteBuffer allTokens;
    private final String containerType;

    public ApplicationMaster(String[] args) throws IOException {
        conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        fileSystem = FileSystem.get(conf);
        containerType = args[0];
        inputPath = args[1];
        outputPath = args[2];
    }

    public void run() throws YarnException, IOException {
        String status = "Application Started";
        FinalApplicationStatus appStatus = FinalApplicationStatus.UNDEFINED;

        try {
/*TODO remove output folder
        LOG.info("Removing outputFolder if present: {}", outputPath);
        fileSystem.delete(new Path(outputPath), true);
*/
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
            capacity.setMemory(49152);
            //capacity.setMemory(8192);
            capacity.setVirtualCores(16);
            //capacity.setVirtualCores(8);

            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);

            LOG.info("containerType = '"+containerType+"'");;
            if (containerType.equals("MyContainerSLS")) {
                inputFile = new Path(inputPath + "/sls-jobs.json");
            } else {
                inputFile = new Path(inputPath);
            }

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
                    LOG.error("InterruptedException : ", e);
                }
            }

            // Join all launched threads: needed for when we time
            // out and we need to release containers
            for (Thread launchThread : launchThreads) {
                try {
                    launchThread.join(10000);

                } catch (InterruptedException e) {
                    LOG.info("Exception thrown in thread join: {}", e);
                }
            }

            LOG.info("Containers have all completed, so shutting down NMClient and AMRMClient...");
            status = "Application complete !";
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } catch (IOException | YarnException e) {
            status = "Application error !";
            appStatus = FinalApplicationStatus.FAILED;
            throw e;
        } finally {
            // stop NM handler
            nmClient.stop();
            // Unregister with ResourceManager
            amRMClient.unregisterApplicationMaster(appStatus, status, null);
            amRMClient.stop();
        }
        //TODO delete the HDFS jar file used by the Application Master
    }

    public BlockLocation[] getBlockLocations() throws IOException {
        // Read the block information from HDFS
        FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
        LOG.info("File {} status = {}",inputFile.toString(), fileStatus.toString());
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
            LOG.error("Application Master main error : ", e);
        }
    }

}
