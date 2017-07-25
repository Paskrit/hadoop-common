package org.apache.hadoop.yarn.sls.yarnapp;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;

public class MyContainerSLS {
    private static final Logger LOG = LoggerFactory
            .getLogger(MyContainerSLS.class);

    private String hostname;
    private YarnConfiguration conf;
    private String inputPath;
    private String outputPath;

    public MyContainerSLS(String[] args) throws IOException {
        hostname = NetUtils.getHostname();
        conf = new YarnConfiguration();
        inputPath = args[0];
        outputPath = args[1];
    }

    private void run() {
        LOG.info("Running Container on {}", this.hostname);

        LOG.info("Addign Ressources {}", getClass().getClassLoader().getResource("hadoop.tar")+"/hadoop-2.6.0-cdh5.11.0/etc/hadoop/core-site.xml");
/*
        conf.addResource(new Path(getClass().getClassLoader().getResource("hadoop.tar")+"/hadoop-2.6.0-cdh5.11.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(getClass().getClassLoader().getResource("hadoop.tar")+"/hadoop-2.6.0-cdh5.11.0/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path(getClass().getClassLoader().getResource("hadoop.tar")+"/hadoop-2.6.0-cdh5.11.0/etc/hadoop/yarn-site.xml"));
        conf.addResource(new Path(getClass().getClassLoader().getResource("hadoop.tar")+"/hadoop-2.6.0-cdh5.11.0/etc/hadoop/sls-runner.xml"));
        conf.reloadConfiguration();
*/
//
//        Map<String, String> env = System.getenv();
//
//        try {
//            for (java.net.URL u : Collections.list(getClass().getClassLoader().getResources(""))
//                    ) {
//                LOG.info(u.toString());
//
//                File f = new File(u.toURI());
//                if (f != null) {
//                    for (File ff : f.listFiles()) {
//                        LOG.info("\tFile {}", ff.getName());
//                    }
//                }
//            }
//            java.net.URL u = getClass().getClassLoader().getResource("hadoop.tar");
//            LOG.info(u.toString());
//
//            Stack<File> s = new Stack<>();
//            s.add(new File(u.toURI()));
//
//            while (!s.empty())
//            {
//                File f = s.pop();
//                if (f != null && f.listFiles() != null) {
//
//                    for (File ff : f.listFiles()) {
//                        if (ff.isDirectory()){s.add(ff);}
//                        String pattern = ff.isDirectory() ? "D" : "F";
//                        LOG.info("File {} {}", pattern, ff.getAbsolutePath());
//                    }
//                }
//            }
//
//
//
//        } catch (IOException | URISyntaxException e) {
//            LOG.error(e.getLocalizedMessage());
//        }

        try {
            LOG.info("Config ClassPath");

//            Process p;
//            try {
//                LOG.info("HADOOP_BASE=`which hadoop`");
//                p = Runtime.getRuntime().exec("HADOOP_BASE=`which hadoop`");
//                p.waitFor();
//                LOG.info("HADOOP_BASE=`dirname $HADOOP_BASE`");
//                p = Runtime.getRuntime().exec("HADOOP_BASE=`dirname $HADOOP_BASE`");
//                p.waitFor();
//                LOG.info("DEFAULT_LIBEXEC_DIR=${HADOOP_BASE}/../libexec");
//                p = Runtime.getRuntime().exec("DEFAULT_LIBEXEC_DIR=${HADOOP_BASE}/../libexec");
//                p.waitFor();
//                LOG.info("HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}");
//                p = Runtime.getRuntime().exec("HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}");
//                p.waitFor();
//                String command = "bash . " + getClass().getClassLoader().getResource("hadoop.tar").toURI() + "/hadoop-2.6.0-cdh5.11.0/libexec/hadoop-config.sh";
//
//                Process pr = new ProcessBuilder("/bin/bash", "-c", ". " + getClass().getClassLoader().getResource("hadoop.tar").toURI() + "/hadoop-2.6.0-cdh5.11.0/libexec/hadoop-config.sh " + ";").start();
//                pr.waitFor();
//
//                LOG.info(command);
//                //p = Runtime.getRuntime().exec(command);
//                //p.waitFor();
//
//                LOG.info("export HADOOP_CLASSPATH=\"${HADOOP_CLASSPATH}:${TOOL_PATH}:html\"");
//
//                pr = new ProcessBuilder("/bin/bash", "-c", "export HADOOP_CLASSPATH=\"${HADOOP_CLASSPATH}:${TOOL_PATH}:html\";").start();pr.waitFor();
//                //p = Runtime.getRuntime().exec("/bin/bash -c export HADOOP_CLASSPATH=\"${HADOOP_CLASSPATH}:${TOOL_PATH}:html\"");
//                //p.waitFor();
//            } catch (Exception e) {
//                LOG.error("Error when trying to Config ClassPath with exec : {}", e.getLocalizedMessage());
//            }

            /// DEBUG
            Map<String, String> env = System.getenv();
            for (String key : env.keySet()) {
                //if (key.contains("HADOOP"))
                {
                    LOG.info("AM : ENV VARIABLE {} = {}", key, env.get(key));
                }
            }
            ///

            LOG.info("Launch SLS Runner");

            String inputSLS = this.inputPath + "/sls-jobs.json";
            String inputNodes = this.inputPath + "/sls-nodes.json";


            SLSRunner sls = new SLSRunner(true, new String[]{inputSLS}, inputNodes, "tmp_0",
                    new HashSet<String>(), false, 5, conf);
            sls.start();
        } catch (ClassNotFoundException e) {
            LOG.error("ClassNotFoundException : {}", e.getLocalizedMessage());
        } catch (Exception e) {
            LOG.error("Exception : {}", e.getLocalizedMessage());
        }

        LOG.info("Check what has been done");

        File f = new File("tmp_0");
        if (f != null) {
            for (File ff : f.listFiles()) {
                LOG.info("File {}", ff.getName());
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Container just started on {}", NetUtils.getHostname());

        try {
            MyContainerSLS container = new MyContainerSLS(args);
            container.run();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        LOG.info("Container is ending...");
    }

}
