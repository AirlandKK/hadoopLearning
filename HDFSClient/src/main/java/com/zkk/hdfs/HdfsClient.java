package com.zkk.hdfs;

/**
 * @author Keke
 * @create 2022-01-05 22:27
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用套路
 * 1.获取一个客户端对象
 * 2.执行相关的操作命令
 * 3.关闭资源
 * HDFS zookeeper
 *         core-site.xml中
 *         fs.defaultFS
 *         value>hdfs://localhost:9000
 *
 */

public class HdfsClient {

    private FileSystem fs;

    @BeforeEach //Before 被 BeforeEach代替了
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接集群nn地址
        URI uri = new URI("hdfs://ZKK01:8020"); //9000 内部通信端口有的可能为8020看core-site.xml设置，外部页面9870
        //创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        //用户
        String user = "root";
        //1.获取到了客户端对象
        fs = FileSystem.get(uri, configuration,user);
    }

    @AfterEach
    public void close() throws IOException {
        //3.关闭资源
        fs.close();
    }

    //创建目录
    @Test
    public void testmkdir() throws  IOException {
        //2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));

    }

    //上传

    /**
     * 参数优先级
     * hdfs-default.xml=>hdfs-site.xml=>在项目资源目录下的配置文件resources=>代码里面的配置
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        // 参数解读：参数一：表示删除原数据 参数二：是否覆盖 参数三：原数据路径 参数四：目的地路径
        fs.copyFromLocalFile(false,true,new Path("F:\\sunwukong1.txt"),new Path("/xiyou2/huaguoshan2"));
    }


    //文件下载
    @Test
    public void testGet() throws IOException {
        //参数的解读：参数一：原文件是否删除 参数二：原文件路径HDFS 参数三；目标地址路径win 参数四：是否开启校验
        //用crc算法进行校验
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan1"),new Path("F:\\"),false);
    }

    //删除
    @Test
    public void testRm() throws IOException {
        //删除文件
        //参数解读：参数1：要删除的路径 参数2：是否递归删除
        fs.delete(new Path("/xiyou/huaguoshan1"),true);
    }

    //文件的更名和移动
    @Test
    public void  testMv() throws IOException {
        //参数解读：参数1：原文件路径 参数二：目标文件的路径
        //对文件名称的修改
        fs.rename(new Path("/xiyou/huaguoshan1/sunwukong1.txt"),new Path("/xiyou/huaguoshan1/sunwukong.txt"));

        //对文件的移动和更名
        fs.rename(new Path("/xiyou/huaguoshan1/sunwukong.txt"),new Path("/ss.txt"));

        //目录的更名
        fs.rename(new Path("/xiyou/huaguoshan1/"),new Path("/xiyou/huaguoshan/"));
    }

    //获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        //获取所有文件信息
        //第二个参数是递归
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //遍历文件
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("==========="+fileStatus.getPath()+"==================");
/*            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockLocations());
            System.out.println(fileStatus.getPath().getName());*/
            System.out.println(fileStatus.toString());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status:listStatus){
            if (status.isFile()){
                System.out.println("文件"+status.getPath().getName());
            }else{
                System.out.println("目录"+status.getPath().getName());
            }
        }
    }
}

