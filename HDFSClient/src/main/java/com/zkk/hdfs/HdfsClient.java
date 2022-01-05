package com.zkk.hdfs;

/**
 * @author Keke
 * @create 2022-01-05 22:27
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
        URI uri = new URI("hdfs://ZKK01:9000"); //9000 内部通信端口有的可能为8020看core-site.xml设置，外部页面9870
        //创建一个配置文件
        Configuration configuration = new Configuration();

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

    @Test
    public void testmkdir() throws  IOException {
        //2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));

    }
}

