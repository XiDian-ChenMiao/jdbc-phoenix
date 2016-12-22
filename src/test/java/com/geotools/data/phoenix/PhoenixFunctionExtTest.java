package com.geotools.data.phoenix;

import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.phoenix.PhoenixDataStoreFactory;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Phoenix函数扩展测试类
 * Created by Administrator on 2016/12/22.
 */
public class PhoenixFunctionExtTest {
    private static Map<String, Object> params = new HashMap<>();

    static {
        params.put(PhoenixDataStoreFactory.DATABASE.key, "hbase-unsecure");
        params.put(PhoenixDataStoreFactory.DBTYPE.key, "phoenix");
        params.put(PhoenixDataStoreFactory.HOST.key, "192.168.3.201");
        params.put(PhoenixDataStoreFactory.PORT.key, 2181);
        params.put(PhoenixDataStoreFactory.USER.key, "root");
        params.put(PhoenixDataStoreFactory.PASSWD.key, "root");
    }

    /**
     * 获取连接对象
     * @return
     */
    private Connection getConnection() {
        Properties properties = new Properties();
        properties.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
        Connection connection = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String url = "jdbc:phoenix:cloudgis1.com:2181:/hbase-unsecure";
            connection = DriverManager.getConnection(url, properties);
            connection.setAutoCommit(true);
        } catch (Exception e) {
            System.out.println("create connection exception : " + e.getMessage());
        }
        return connection;
    }

    /**
     * 生成创建函数的SQL
     * @param funcNameAndParam 函数名及参数
     * @param returnType 返回值类型
     * @param ref 函数类全名
     * @param location 函数在集群中的存放地址
     * @return
     */
    private String generateCreateFunctionSql(String funcNameAndParam, String returnType, String ref, String location) {
        return  "CREATE FUNCTION " + funcNameAndParam + " RETURNS " + returnType + " AS '" + ref + "' USING jar '" + location + "'";
    }

    /**
     * 创建函数
     * @param funcNameAndParam
     * @param returnType
     * @param ref
     * @param location
     * @throws IOException
     */
    private void createFunction(String funcNameAndParam, String returnType, String ref, String location) throws IOException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection();
            statement = connection.createStatement();
            statement.execute(generateCreateFunctionSql(funcNameAndParam, returnType, ref, location));
        } catch (Exception e) {
            System.out.println("create function exception : " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 取消指定函数
     * @param funcName 函数名称
     */
    private void dropFunction(String funcName) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection();
            statement = connection.createStatement();
            statement.execute(generateDropFunctionSql(funcName));
        } catch (Exception e) {
            System.out.println("drop function exception : " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 生成取消函数的SQL
     * @param funcName
     * @return
     */
    private String generateDropFunctionSql(String funcName) {
        return "DROP FUNCTION IF EXISTS " + funcName;
    }

    /**
     * 创建ST_REVERSE函数测试
     */
    @Test
    public void testCreateReverseFunction() {
        try {
            createFunction("ST_REVERSE(VARCHAR)", "VARCHAR", "org.geotools.data.phoenix.function.ReverseFunction", "hdfs://cloudgis/apps/hbase/data/lib/phoenix-functions-ext.jar");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建ST_DISTANCE函数测试
     */
    @Test
    public void testCreateDistanceFunction() {
        try {
            createFunction("ST_DISTANCE(VARCHAR, VARCHAR)", "DOUBLE", "org.geotools.data.phoenix.function.DistanceFunction", "hdfs://cloudgis/apps/hbase/data/lib/phoenix-functions-ext.jar");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 取消ST_REVERSE函数测试
     */
    @Test
    public void testDropReverseFunction() {
        dropFunction("ST_REVERSE");
    }

    /**
     * 取消ST_DISTANCE函数测试
     */
    @Test
    public void testDropDistanceFunction() {
        dropFunction("ST_DISTANCE");
    }

    /**
     * 使用ST_DISTANCE函数测试
     */
    @Test
    public void testUseDistanceFunction() throws IOException, FilterToSQLException, CQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;
        try {
            connection = getConnection();
            statement = connection.createStatement();
            result = statement.executeQuery("SELECT * FROM GEOTOOLS_CM WHERE ST_DISTANCE(GEOMETRY, 'POINT (0 0)') > 1.0");
            while (result.next()) {
                System.out.println(result.getString(1));
            }
        } catch (Exception e) {
            System.out.println("create function exception : " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 使用ST_REVERSE函数测试
     */
    @Test
    public void testUseReverseFunction() {
        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;
        try {
            connection = getConnection();
            statement = connection.createStatement();
            result = statement.executeQuery("SELECT ST_REVERSE(ST_REVERSE(NAME)) FROM GEONAME");
            while (result.next()) {
                System.out.println(result.getString(1));
            }
        } catch (Exception e) {
            System.out.println("create function exception : " + e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 主函数
     * @param args
     */
    public static void main(String[] args) throws IOException, FilterToSQLException, CQLException {
        PhoenixFunctionExtTest test = new PhoenixFunctionExtTest();
        test.testUseDistanceFunction();
    }
}
