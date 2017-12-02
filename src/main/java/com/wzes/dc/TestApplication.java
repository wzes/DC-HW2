package com.wzes.dc;

import com.wzes.dc.util.GzipUtils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Create by xuantang
 * @date on 11/14/17
 */
public class TestApplication {
    public static void main(String[] args) {
//        File file = new File("/home/xuantang/Downloads/dictionary.sql");
//        BufferedReader readFile = null;
//        FileReader fileReader = null;
//
//        BufferedWriter writeFile = null;
//        FileWriter fileWriter = null;
//        try {
//            fileReader = new FileReader("/home/xuantang/Downloads/dictionary.sql");
//            readFile = new BufferedReader(fileReader);
//
//            fileWriter = new FileWriter("/home/xuantang/Downloads/dictionary1.csv");
//            writeFile = new BufferedWriter(fileWriter);
//            String line;
//            int i = 0;
//            while ((line = readFile.readLine()) != null) {
//                line = line.replace(");", "")
//                        .replace("INSERT INTO dictionary(pinyin, word, firstLetters, " +
//                                "defaultSort, frequence, latestFlag, createTime, modifyTime) VALUES (", "")
//                        .replace("INSERT INTO `dictionary` VALUES (", "");
//                //System.out.println(line + "\n");
//                writeFile.write(line + "\n");
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                fileReader.close();
//                readFile.close();
//                writeFile.close();
//                fileWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        }
        File inFile = new File("/d1/input/slide.db");
        File outFile = new File("/d1/input/slide.zip");
        try {
            FileInputStream fileInputStream = new FileInputStream(inFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);

            int len = fileInputStream.available();
            byte[] conent = new byte[len];
            byte[] buf = new byte[1024];
            int count;
            int index = 0;
            while ((count = bufferedInputStream.read(buf)) != -1) {
                System.arraycopy(buf, 0, conent, index*1024, count);
                index++;
            }
            byte[] compress = GzipUtils.compress(conent);

            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outFile));

            assert compress != null;
            bufferedOutputStream.write(compress);

            bufferedOutputStream.close();
            bufferedInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//      File file = new File("iframe.html");

//        String url_path = "http://www.sse.com.cn/assortment/stock/list/share/";
//        String root_path = null;
//        try {
//            URL url = new URL(url_path);
//            root_path = url.getProtocol() + "://" + url.getHost();
//            System.out.println(root_path);
//        } catch (MalformedURLException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            FileReader fileReader = new FileReader("iframe.html");
//            BufferedReader bufferedReader = new BufferedReader(fileReader);
//            String line;
//            StringBuilder stringBuilder = new StringBuilder();
//            while ((line = bufferedReader.readLine()) != null) {
//                stringBuilder.append(line).append('\n');
//            }
//            String content = stringBuilder.toString();
//            int index = content.indexOf("href=\"/");
//            System.out.println(index);
//            String regEx1="href=\"/";
//            Pattern pattern1 = Pattern.compile(regEx1);
//            Matcher matcher1 = pattern1.matcher(content);
//            String r1 = matcher1.replaceAll("href=\"" + root_path + "/");
//            String regEx2="src=\"/";
//            Pattern pattern2 = Pattern.compile(regEx2);
//            Matcher matcher2 = pattern2.matcher(r1);
//            String r2 = matcher2.replaceAll("src=\"" + root_path + "/");
//            System.out.println(r2);
//
//
//            bufferedReader.close();
//            fileReader.close();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static String replaceAll(String urlPath, String xml) {

        String url_path = urlPath;
        String root_path = null;
        try {
            URL url = new URL(url_path);
            root_path = url.getProtocol() + "://" + url.getHost();
            System.out.println(root_path);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }


        String regEx1="href=\"/";
        Pattern pattern1 = Pattern.compile(regEx1);
        Matcher matcher1 = pattern1.matcher(xml);
        String r1 = matcher1.replaceAll("href=\"" + root_path + "/");
        String regEx2="src=\"/";
        Pattern pattern2 = Pattern.compile(regEx2);
        Matcher matcher2 = pattern2.matcher(r1);
        String r2 = matcher1.replaceAll("src=\"" + root_path + "/");
        return r2;
    }
}
