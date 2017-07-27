package org.apache.hadoop.yarn.sls.yarnapp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by k.jacquemin on 27/07/17.
 */
public class BufferedStreamFactory {

    private static final Logger LOG = LoggerFactory
            .getLogger(MyContainerSLS.class);

    private boolean useHdfsStreamMode = false;
    private Configuration hdfsConfiguration;
    private FileSystem fs;

    private BufferedStreamFactory() {
    }

    /**
     * Instance unique non préinitialisée
     */
    private static BufferedStreamFactory INSTANCE = null;

    /**
     * Point d'accès pour l'instance unique du singleton
     */
    public static BufferedStreamFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new BufferedStreamFactory();
        }
        return INSTANCE;
    }

    public void setHdfsStreamMode(Boolean useHdfs, Configuration config) throws IOException {
        useHdfsStreamMode = useHdfs;
        hdfsConfiguration = config;
        fs = FileSystem.get(config);
    }

    private BufferedWriter createHdfsBufferedWritter(String path) throws IOException {
        LOG.info("Create HdfsBufferedWritter : "+path);
        BufferedWriter writer;
        try{
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path), true)));
        }
        catch (IOException e){
            LOG.error("Error Create HdfsBufferedWritter : "+path, e);
            throw e;
        }
        return writer;
    }

    public BufferedWriter createBufferedWritter(String path) throws IOException {
        return createHdfsBufferedWritter(path);
    }

    public boolean createDirectoryIfNotExist(String path) {
        boolean result = false;
        if (useHdfsStreamMode) {
            try {
                Path dir = new Path(path);
                result = fs.exists(dir) ? true : fs.mkdirs(dir);
            } catch (IOException e) {
                LOG.error("Cannot Create HDFS directory : ", e);
            }
        } else {
            File dir = new File(path);
            result = dir.exists() ? true : dir.mkdirs();
        }
        return result;
    }

}
