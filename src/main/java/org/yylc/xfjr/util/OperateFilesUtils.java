package org.yylc.xfjr.util;

import java.io.*;
import java.util.List;

public class OperateFilesUtils {

    //  使用递归的方法遍历文件夹下所有文件
    public static void traverseFolder(String path) {
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder(file2.getAbsolutePath());
                    } else {
                        String file2path = file2.getAbsolutePath();
                        System.out.println("文件:" + file2.getAbsolutePath());
                        try {
                            String s = readText1(file2path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }

    //  按行读取文件
    public static String readText1(String path) throws IOException {
        //BufferedReader是可以按行读取文件
//        FileInputStream inputStream = new FileInputStream("d://a.txt");
        FileInputStream inputStream = new FileInputStream(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String str = null;
        while((str = bufferedReader.readLine()) != null)
        {
            System.out.println(str);
        }

        //close
        inputStream.close();
        bufferedReader.close();
        return str;
    }


    /***************************************************************************
     * <b>function:</b> 在java中 路径/需要替换成//
     * @param file 替换的文件
     * @return 替换后的路径
     */
    private static String getAvailablePath(File file) {
        return file.getPath().replace("//", "/");
    }

    /***************************************************************************
     * <b>function:</b> 返回某个文件夹下所有的文件
     * @param fileList 找到的文件集合
     * @param path 路径
     * @param fileSuffix 要找的文件后缀
     */
    public static List<File> getFiles(List<File> fileList, String path, String fileSuffix) {
        File file = new File(path);
        File[] files = file.listFiles();
        if (files == null) {
            return fileList;
        } else {
            for (int i = 0; i < files.length; i++) {
                // 判断是否是文件夹
                if (files[i].isDirectory()) {
                    // 递归调用getFiles方法，得到所有的文件
                    getFiles(fileList, getAvailablePath(files[i]), fileSuffix);
                } else if (files[i].getName().lastIndexOf(fileSuffix) != -1) {// 只处理fileSuffix后缀的文档
                    // copyFileAndAddPackageName(files[i]);
                    fileList.add(files[i]);// 添加到文件集合中
                }
                // System.out.println(files[i].getAbsolutePath().replace('//',
                // '/'));
            }
        }
        return fileList;
    }

    /***************************************************************************
     * <b>function:</b> 把文件内容复制到指定文件中
     * @param toFile 保存的文件
     */
    public static void copyFile2TempFile(List<File> fileList, File toFile) {
        BufferedWriter bw = null;
        BufferedReader br = null;
        FileWriter fileWriter = null;
        FileReader fileReader = null;
        try {
            StringBuilder sb = new StringBuilder();
            String content = "";
            for (File textFile : fileList) {
                System.out.println(textFile.getName());
                fileReader = new FileReader(textFile);// 读取文件的内容
                br = new BufferedReader(fileReader);
                while ((content = br.readLine()) != null) {
                    System.out.println(content);
                    if (!content.trim().equals("")) {
                        sb.append(content);// .append("/r");
                    }
                }
            }
            fileWriter = new FileWriter(toFile);// 向文件中写入刚才读取文件中的内容
            bw = new BufferedWriter(fileWriter);
            bw.write(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
                br.close();
                fileWriter.close();
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /***************************************************************************
     *  <b>function:</b> 读取单个文件
     * @param file 读取内容的文件
     * @param toFile 保存后的文件
     */
    public static void copyFile2TempFile(File file, File toFile) {
        BufferedWriter bw = null;
        BufferedReader br = null;
        FileWriter fileWriter = null;
        FileReader fileReader = null;
        try {
            StringBuilder sb = new StringBuilder();
            String content = "";
            fileReader = new FileReader(file);// 读取文件的内容
            br = new BufferedReader(fileReader);
            while ((content = br.readLine()) != null) {
                System.out.println(content);
                if (!content.trim().equals("")) {
                    sb.append(content);// .append("/r");
                }
            }
            fileWriter = new FileWriter(toFile);// 向文件中写入刚才读取文件中的内容
            bw = new BufferedWriter(fileWriter);
            bw.write(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
                br.close();
                fileWriter.close();
                fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        /*List<File> fileList = new ArrayList<File>();
        getFiles(fileList, "c:/txt_test", ".txt");
        getFiles(fileList, "c:/txt", ".txt");
        // 将读取的内容放入tempFile中
        File tempFile = new File("c://temp.txt");
        if (!tempFile.exists()) {
            try {
                //不存在就创建一个
                tempFile = File.createTempFile("temp", ".txt", new File("c://"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
        //copyFile2TempFile(fileList, tempFile);
//        copyFile2TempFile(new File("c://a.txt"), tempFile);
        traverseFolder("E:\\git\\spark-etl\\data\\json");
//        traverseFolder("E:\\git\\spark-etl\\data\\json\\0c53547271974bcd93fde1bb6a8c477f.txt");
    }
}