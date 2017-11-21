package com.wzes.dc.produce;

import com.wzes.dc.bean.Task;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BufferedRandomAccessFile;
import com.wzes.dc.util.BytesUtils;
import com.wzes.dc.util.GzipUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
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
    private static final int BLOCK_SIZE = 128;

    private static final int MAX_NUM = 2014 * 512;

    private static final int TIMES = 256;

    private static final int INT_SIZE = 3;

    public Producer() {

    }

    /**
     * produce data and write it to file
     * @param filename the filename of des file
     */
    public void writeToFileByCompress(String filename) {
        FileOutputStream fileOutputStream;
        BufferedOutputStream bufferedOutputStream = null;
        DataOutputStream dataOutputStream = null;
        // how many data write to file one time.
        byte[] numbers = new byte[TIMES * INT_SIZE * BLOCK_SIZE];
        try {
            try {
                fileOutputStream = new FileOutputStream(new File(filename));
                dataOutputStream = new DataOutputStream(fileOutputStream);
                bufferedOutputStream = new BufferedOutputStream(dataOutputStream);
                // calculate time
                // long start = System.currentTimeMillis();
                int dev = 0;
                // Long s = 0L;
                for(int i = 1; i <= MAX_NUM; i++) {
                    byte[] bytes = BytesUtils.intToByteArray(i);
                    for( int j = 0; j < TIMES; j++) {
                        System.arraycopy(bytes, 0, numbers, j * INT_SIZE + dev * INT_SIZE * BLOCK_SIZE, INT_SIZE);
                    }
                    dev++;
                    if(i % BLOCK_SIZE == 0) {
                        dev = 0;
                        // compress
                        byte[] compressData = GzipUtils.compress(numbers);
                        bufferedOutputStream.write(compressData);
                        //int len = compressData.length;
                        // new task
                        // Task task = new Task(s, len);
                        // s += len;
                        // add task to queue
                        // TaskQueue.getInstance().addTask(task);
                    }
                }
                // TaskQueue.getInstance().setProduceEnd(true);
                // long end = System.currentTimeMillis();
                // print total time
                // System.out.println("produce over ! total time: " +  (end - start) + " ms");
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
        //producer.writeByBufferedOutput("test");
        producer.writeByBufferedRandom("test");
    }


    public void writeByBufferedOutput(String filename) {
        long start = System.currentTimeMillis();
        FileOutputStream fileOutputStream;
        BufferedOutputStream bufferedOutputStream = null;
        DataOutputStream dataOutputStream = null;
        // how many data write to file one time.
        byte[] numbers = new byte[TIMES * INT_SIZE];
        try {
            try {
                fileOutputStream = new FileOutputStream(new File(filename));
                dataOutputStream = new DataOutputStream(fileOutputStream);
                bufferedOutputStream = new BufferedOutputStream(dataOutputStream);
                // calculate time
                for(int i = 1; i <= MAX_NUM; i++) {
                    byte[] bytes = BytesUtils.intToThreeByteArray(i);
                    for( int j = 0; j < TIMES; j++) {
                        System.arraycopy(bytes, 0, numbers, j * INT_SIZE, INT_SIZE);
                    }
                    bufferedOutputStream.write(numbers);
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                bufferedOutputStream.flush();

                bufferedOutputStream.close();
                long end = System.currentTimeMillis();
                // print total time
                System.out.println("produce over ! total time: " +  (end - start) + " ms");
            }
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * Write to file Using nio
     * @param filename
     */
    public void writeByBufferedRandom(String filename) {
        //long start = System.currentTimeMillis();
        // FileOutputStream fileOutputStream = null;
        BufferedRandomAccessFile bufferedRandomAccessFile = null;
        // RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        byte[] numbers = new byte[TIMES * INT_SIZE];
        File file = new File(filename);

        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            bufferedRandomAccessFile = new BufferedRandomAccessFile(PRODUCE_FILENAME, "rw", 10);
            fileChannel = bufferedRandomAccessFile.getChannel();
            //fileChannel = randomAccessFile.getChannel();
            //fileOutputStream = new FileOutputStream(file);
            //fileChannel = fileOutputStream.getChannel();
            //Long s = 0L;
            //ByteBuffer byteBuffer = ByteBuffer.allocate(NUM_SIZE);
            MappedByteBuffer mbbo = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_NUM*TIMES*INT_SIZE);
            for(int i = 1; i <= MAX_NUM; i++) {
                //　使用函数会变慢　byte[] bytes = BytesUtils.intToThreeByteArray(i);
                // 一个int对应三位byte
                byte one = (byte) ((i >> 16) & 0xFF);
                byte two = (byte) ((i >> 8) & 0xFF);
                byte three = (byte) (i & 0xFF);

                //　这种复制方式比System.arraycopy快
                for( int j = 0; j < TIMES; j++) {
//                    numbers[j * INT_SIZE] = bytes[0];
//                    numbers[j * INT_SIZE + 1] = bytes[1];
//                    numbers[j * INT_SIZE + 2] = bytes[2];
                    //　单个写更快
                    mbbo.put(one);
                    mbbo.put(two);
                    mbbo.put(three);
                    //bufferedRandomAccessFile.write(bytes);
                    //　比较慢
                    //System.arraycopy(bytes, 0, numbers, j * INT_SIZE, INT_SIZE);
                }
                //mbbo.put(bytes);
//                byteBuffer.clear();
//                byteBuffer.put(numbers);
//                byteBuffer.flip();
                //mbbo.put(byteBuffer);
//                while(byteBuffer.hasRemaining()) {
//                    fileChannel.write(byteBuffer);
//                }

//                byte buf[] = new byte[1024];
//                int readcount;
                //bufferedRandomAccessFile.write(numbers, 0, numbers.length);
//                while((readcount = bufferedRandomAccessFile.read(buf)) != -1) {
//                    bufferedRandomAccessFile.write(buf, 0, readcount);
//                }

            }
            fileChannel.close();
            bufferedRandomAccessFile.close();
            //randomAccessFile.close();
            //bufferedRandomAccessFile.close();
            //TaskQueue.getInstance().setProduceEnd(true);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // end = System.currentTimeMillis();
        // print total time
        //System.out.println(Thread.currentThread().toString() + " Produce over: " +  (end - start) + " ms");
    }


    public void writeByBufferedWrite(String filename) {
        //long start = System.currentTimeMillis();
        OutputStreamWriter outputStreamWriter = null;
        BufferedWriter bufferedWriter = null;
        byte[] numbers = new byte[TIMES * INT_SIZE];
        File file = new File(filename);

        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            outputStreamWriter = new OutputStreamWriter(new FileOutputStream(file));
            bufferedWriter = new BufferedWriter(outputStreamWriter);
            for(int i = 1; i <= MAX_NUM; i++) {
                //　使用函数会变慢　
                for( int j = 0; j < TIMES; j++) {
                    bufferedWriter.write(j);
                }
            }
            outputStreamWriter.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // end = System.currentTimeMillis();
        // print total time
        //System.out.println(Thread.currentThread().toString() + " Produce over: " +  (end - start) + " ms");
    }
}
