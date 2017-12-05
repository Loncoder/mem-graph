package ict.ada.gdb.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lon on 16-3-18.
 */
public class FileUtils {

    /**
     * 创建文件，若文件夹不存在则自动创建文件夹，若文件存在则删除旧文件
     *
     * @param path :待创建文件路径
     */
    public boolean createNewFile(String path) {
        boolean result = true;
        File file = new File(path);
        try {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            result = false;
        }
        return result;
    }

    /**
     * 将文件输入流写入文件
     */
    public boolean writeFileFromInputStream(InputStream inStream, String path) {
        boolean result = true;
        try {
            File file = new File(path);
            if (!file.exists() && createNewFile(path)) return false;
            FileOutputStream out = new FileOutputStream(file);
            byte[] data = new byte[1024];
            int num;
            while ((num = inStream.read(data, 0, data.length)) != -1) {
                out.write(data, 0, num);
            }
            out.close();
        } catch (Exception e) {
            result = false;
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获取文件输入流
     */
    public InputStream readFileToInputStream(String path) {
        InputStream inputStream = null;
        try {
            File file = new File(path);
            inputStream = new FileInputStream(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputStream;
    }

    /**
     * 根据给出路径自动选择删除文件或整个文件夹
     *
     * @param path :文件或文件夹路径
     */
    public void deleteFiles(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();// 删除文件
        } else {
            File[] subFiles = file.listFiles();
            for (File subfile : subFiles) {
                deleteFiles(subfile.getAbsolutePath());// 删除当前目录下的子目录
            }
            file.delete();// 删除当前目录
        }
    }

    /**
     * 遍历目录及其子目录下的所有文件并保存
     *
     * @param path       目录全路径
     * @param myFileList 列表：保存文件对象
     */
    public void listDirectory(String path, List<String> myFileList, String flag) {
        File pathFile = new File(path);
        if (!pathFile.exists()) {
            System.out.println("文件名称不存在!");
        } else {
            if (pathFile.isFile()) {
                String absPath = pathFile.getAbsolutePath();
                if (absPath.contains(flag)) {
                    System.out.println("file:\t" + pathFile.getAbsolutePath());
                    myFileList.add(pathFile.getAbsolutePath());
                }
            } else {
                File[] files = pathFile.listFiles();
                for (int i = 0; i < files.length; i++) {
                    listDirectory(files[i].getAbsolutePath(), myFileList, flag);
                }
            }
        }
    }

    public List<String> readLinesFromFile(String path) {
        List<String> lines = new ArrayList<String>(1024);
        InputStream inputStream = readFileToInputStream(path);
        if (inputStream == null) return lines;
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String dataLine;
        try {
            while ((dataLine = br.readLine()) != null) {
                lines.add(dataLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

}
