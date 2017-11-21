package com.wzes.dc.consume;

import com.wzes.dc.produce.Producer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * @author Create by xuantang
 * @date on 11/19/17
 */
public class HDWriter {
    public static final String PRODUCE_FILENAME = "produce.zip";
    private static final String OUT_PATH = "hdfs://192.168.1.107:9000/test/out.zip";

    public static void main(String[] args) {
        try {
            // produce data
            long produceStartTime = System.currentTimeMillis();
            Producer producer = new Producer();
            producer.writeToFileByCompress(PRODUCE_FILENAME);
            long produceStopTime = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " Produce Time: " +  (produceStopTime - produceStartTime) + " ms");
            // writer to hdfs
            long writeStartTime = System.currentTimeMillis();
            writeToHdfs(PRODUCE_FILENAME, OUT_PATH);
            long writeStopTime = System.currentTimeMillis();
            System.out.println(Thread.currentThread().toString() + " Write Time: " +  (writeStopTime - writeStartTime) + " ms");

            System.out.println(Thread.currentThread().toString() + " Total Time: " +  (writeStopTime - produceStartTime) + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param local
     * @param hdfs
     * @throws IOException
     */
    public static void writeToHdfs(String local, String hdfs)
            throws IOException {
        Configuration config = new Configuration();
        config.addResource("conf/core-site.xml");

        // get file system
        FileSystem fileSystem = FileSystem.get(URI.create(hdfs), config);
        // get file input stream
        FileInputStream fileInputStream = new FileInputStream(new File(local));
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(hdfs));
        IOUtils.copyBytes(bufferedInputStream, fsDataOutputStream, 4096, true);
        fileInputStream.close();
        fsDataOutputStream.close();
        bufferedInputStream.close();
    }
}
