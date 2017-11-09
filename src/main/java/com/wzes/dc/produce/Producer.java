package com.wzes.dc.produce;

import com.wzes.dc.bean.Task;
import com.wzes.dc.service.TaskQueue;
import com.wzes.dc.util.BytesUtils;
import com.wzes.dc.util.GzipUtils;

import java.io.*;

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
    private static final int BLOCK_SIZE = 64;

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
                        // System.arraycopy(bytes, 0, numbers, j * 4, 4);
                        // GzipUtils.compress(numbers);
                        //bufferedOutputStream.write(GzipUtils.compress(numbers));
                        //bufferedOutputStream.write(i);
                    }
                    dev++;
                    if(i % BLOCK_SIZE == 0) {
                        dev = 0;
                        // compress
                        byte[] compressData = GzipUtils.compress(numbers);
                        bufferedOutputStream.write(compressData);
                        int len = compressData.length;
                        Task task = new Task(s, len);
                        s += len;
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
        producer.writeToFile(PRODUCE_FILENAME);
    }

}
