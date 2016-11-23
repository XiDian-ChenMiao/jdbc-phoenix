package com.geotools.data.phoenix;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.phoenix.PhoenixDataStoreFactory;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件描述：Phoenix数据库的简单测试类
 * 创建作者：陈苗
 * 创建时间：2016/11/12 20:49
 */
public class PhoenixDataStoreFactoryTest {

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String url = "jdbc:phoenix:192.168.3.201:2181:/hbase-unsecure";
        Connection conn = DriverManager.getConnection(url);
        if (conn == null)
            System.out.println("连接失败");
        else
            System.out.println("连接成功");
        try {
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @throws IOException
     */
    public static void testFactory() throws IOException {
        PhoenixDataStoreFactory factory = new PhoenixDataStoreFactory();
        Map<String, Object> params = new HashMap<>();

        params.put(PhoenixDataStoreFactory.DATABASE.key, "hbase-unsecure");
        params.put(PhoenixDataStoreFactory.DBTYPE.key, "phoenix");
        params.put(PhoenixDataStoreFactory.HOST.key, "192.168.3.201");
        params.put(PhoenixDataStoreFactory.PORT.key, 2181);

        if (factory.canProcess(params)) {
            DataStore dataStore = DataStoreFinder.getDataStore(params);
            if (dataStore != null) {
                String[] types = dataStore.getTypeNames();
                for (String type : types)
                    System.out.println(type);
            }
        } else {
            System.out.println("Connection Failure.");
        }
    }
}
