package org.apache.hadoop.yarn.sls.yarnapp;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.yarn.sls.RumenToSLSConverter;

public class MyContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(MyContainer.class);

    private String hostname;
    private YarnConfiguration conf;
    private FileSystem fs;
    private Path inputFile;
    private String outputPath;

    public MyContainer(String[] args) throws IOException {
        hostname = NetUtils.getHostname();
        conf = new YarnConfiguration();
        fs = FileSystem.get(conf);
        inputFile = new Path(args[0]);
        outputPath = args[1];
    }

    private void run() {
        LOG.info("Running Container on {}", this.hostname);

        Path outputJobsFile = new Path(this.outputPath + "/sls-jobs.json");
        Path outputNodesFile = new Path(this.outputPath + "/sls-nodes.json");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inputFile)));
             BufferedWriter writerJobs = new BufferedWriter(
                     new OutputStreamWriter(fs.create(outputJobsFile, true)));
             BufferedWriter writerNodes = new BufferedWriter(
                     new OutputStreamWriter(fs.create(outputNodesFile, true)));
        ) {
            LOG.info("Generate SLS Jobs file");
            RumenToSLSConverter.generateSLSLoadFile(reader, writerJobs, LOG);
            LOG.info("Generate SLS Nodes file");
            RumenToSLSConverter.generateSLSNodeFile(writerNodes, LOG);
        } catch (IOException e) {
            LOG.error("IOException : {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        LOG.info("Container just started on {}", NetUtils.getHostname());

        try {
            MyContainer container = new MyContainer(args);
            container.run();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        LOG.info("Container is ending...");
    }

}
