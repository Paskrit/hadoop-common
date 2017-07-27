package org.apache.hadoop.yarn.sls.yarnapp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Map;

public class MyContainerSLS {
    private static final Logger LOG = LoggerFactory
            .getLogger(MyContainerSLS.class);

    private String hostname;
    private String inputPath;
    private String outputPath;

    public MyContainerSLS(String[] args) throws IOException {
        hostname = NetUtils.getHostname();

        inputPath = args[0];
        outputPath = args[1];
    }

    private void dumpOutDebugInfo() {

        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
        }

        String lines = null;
        try {
            lines = Shell.execCommand("ls", "-al");
            //lines = Shell.execCommand("/bin/cat", "launch_container.sh");
            try (BufferedReader buf = new BufferedReader(new StringReader(lines))) {
                String line = "";
                System.out.println("launch_container.sh :");
                while ((line = buf.readLine()) != null) {
                    //LOG.info("System CWD content: " + line);
                    System.out.println("\t" + line);
                }
                System.out.println("-----------------------------------");
            }
        } catch (IOException e) {
            LOG.error("dumpOutDebugInfo : ", e);
        }
    }

    private void run() throws Exception {
        LOG.info("Running Container on {}", this.hostname);

        dumpOutDebugInfo();

        Configuration config = new Configuration(false);
        config.addResource("custom-site.xml");

        BufferedStreamFactory.getInstance().setHdfsStreamMode(true, config);

        String inputSLS = this.inputPath + "/sls-jobs.json";
        String inputNodes = this.inputPath + "/sls-nodes.json";

        LOG.info("Launch SLS Runner with {} and {}", inputSLS, inputNodes);

        try {
            SLSRunner sls = new SLSRunner(true, new String[]{inputSLS}, inputNodes, "tmp_0",
                    new HashSet<String>(), false, 10, config);
            sls.start();
        } catch (Exception e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Container just started on {}", NetUtils.getHostname());

        try {
            MyContainerSLS container = new MyContainerSLS(args);
            container.run();
        } catch (Exception e) {
            LOG.error("Container main error : ", e);
            throw (e);
        }
        LOG.info("Container is ending...");
    }

}
