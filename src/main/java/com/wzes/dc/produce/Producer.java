package com.wzes.dc.produce;

import com.wzes.dc.bean.Task;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BytesUtils;
import com.wzes.dc.util.GzipUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Create by xuantang
 * @date on 11/6/17
 */
public class Producer {

    /**
     * the name of produce file
     */
    public static final String PRODUCE_FILENAME = "produce.bin";

    private static final int NUM_SIZE = 1024;

    /**
     * the size of writting to file everytime
     */
    private static final int BLOCK_SIZE = 256;

    private static final int MAX_NUM = 2014 * 512;

    private static final int TIMES = 256;

    public Producer() {

    }

    /**
     * produce data and write it to file
     * @param filename the filename of des file
     */
    public void writeToFile(String filename) {
        FileOutputStream fileOutputStream;
        BufferedOutputStream bufferedOutputStream = null;
        // how many data write to file one time.
        byte[] numbers = new byte[NUM_SIZE*BLOCK_SIZE];
        try {
            try {
                fileOutputStream = new FileOutputStream(new File(filename));
                bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                // calculate time
                long start = System.currentTimeMillis();
                int dev = 0;
                Long s = 0L;
                for(int i = 1; i <= MAX_NUM; i++) {
                    byte[] bytes = BytesUtils.intToByteArray(i);
                    for( int j = 0; j < TIMES; j++) {
                        System.arraycopy(bytes, 0, numbers, j * 4 + dev*NUM_SIZE, 4);
                    }
                    dev++;
                    if(i % BLOCK_SIZE == 0) {
                        dev = 0;
                        // compress
                        byte[] compressData = GzipUtils.compress(numbers);
                        bufferedOutputStream.write(compressData);
                        int len = compressData.length;
                        // new task
                        Task task = new Task(s, len);
                        s += len;
                        // add task to queue
                        TaskQueue.getInstance().addTask(task);
                    }
                }
                TaskQueue.getInstance().setProduceEnd(true);
                long end = System.currentTimeMillis();
                // print total time
                System.out.println("produce over ! total time: " +  (end - start) + " ms");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                bufferedOutputStream.flush();
                bufferedOutputStream.close();
            }
        }catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        Producer producer = new Producer();
        producer.writeByNormal("test");
        //producer.writeByNIO("test");
    }


    public void writeByNormal(String filename) {
        FileOutputStream fileOutputStream;
        BufferedOutputStream bufferedOutputStream = null;
        // how many data write to file one time.
        byte[] numbers = new byte[NUM_SIZE];
        try {
            try {
                fileOutputStream = new FileOutputStream(new File(filename));
                bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                // calculate time
                long start = System.currentTimeMillis();
                for(int i = 1; i <= MAX_NUM; i++) {
                    byte[] bytes = BytesUtils.intToByteArray(i);
                    for( int j = 0; j < TIMES; j++) {
                        System.arraycopy(bytes, 0, numbers, j * 4, 4);
                    }
                    bufferedOutputStream.write(numbers);
                }
                long end = System.currentTimeMillis();
                // print total time
                System.out.println("produce over ! total time: " +  (end - start) + " ms");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                bufferedOutputStream.flush();
                bufferedOutputStream.close();
            }
        }catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Write to file Using nio
     * @param filename
     */
    public void writeByNIO(String filename) {
        FileOutputStream fileOutputStream = null;
        FileChannel fileChannel = null;
        byte[] numbers = new byte[NUM_SIZE];
        File file = new File(filename);

        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileOutputStream = new FileOutputStream(file);
            fileChannel = fileOutputStream.getChannel();
            long start = System.currentTimeMillis();
            int dev = 0;
            //Long s = 0L;
            ByteBuffer byteBuffer = ByteBuffer.allocate(NUM_SIZE);
            for(int i = 1; i <= MAX_NUM; i++) {
                byte[] bytes = BytesUtils.intToByteArray(i);
                for( int j = 0; j < TIMES; j++) {
                    System.arraycopy(bytes, 0, numbers, j * 4, 4);
                }
                byteBuffer.clear();
                byteBuffer.put(numbers);
                byteBuffer.flip();
                while(byteBuffer.hasRemaining()) {
                    fileChannel.write(byteBuffer);
                }
                //dev++;
//                if(i % BLOCK_SIZE == 0) {
//                    dev = 0;
//                    byteBuffer.clear();
//                    byteBuffer.put(numbers);
//                    byteBuffer.flip();
//                    while(byteBuffer.hasRemaining()) {
//                        fileChannel.write(byteBuffer);
//                    }
//                    //fileChannel.force(true);
//                    // compress
//                    //byte[] compressData = GzipUtils.compress(numbers);
//                    //bufferedOutputStream.write(compressData);
//                    //int len = compressData.length;
//                    // new task
//                    //Task task = new Task(s, len);
//                    //s += len;
//                    // add task to queue
//                    //TaskQueue.getInstance().addTask(task);
//                }
            }
            fileChannel.close();
            fileOutputStream.close();
            //TaskQueue.getInstance().setProduceEnd(true);
            long end = System.currentTimeMillis();
            // print total time
            System.out.println("produce over ! total time: " +  (end - start) + " ms");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
