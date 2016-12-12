package com.geotools.data.phoenix;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;

/*
 * 数据库连接池
 */
public class JdbcPool implements DataSource {
    //用LinkedList来存储生成的connection对象
    private static LinkedList<Connection> connList = new LinkedList<Connection>();

    static {
        //从属性文件中读取连接数据库所要用到的属性
        InputStream input = JdbcPool.class.getClassLoader().getResourceAsStream("jdbc-mysql.properties");
        Properties properties = new Properties();
        try {
            properties.load(input);
            String driver = properties.getProperty("driver");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String url = properties.getProperty("url");
            //数据库连接池的初始化连接数大小
            int connSize = Integer.parseInt(properties.getProperty("initial_connect_size"));
            //加载数据库驱动
            Class.forName(driver);
            for (int i = 0; i < connSize; i++) {
                Connection conn = DriverManager.getConnection(url, username, password);
                System.out.println("获取了数据库连接：" + conn);
                //将获取到的数据库连接加入到connList集合中，connList集合此时就是一个存放了数据库连接的连接池
                connList.add(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取数据库连接，利用对数据库连接的代理进行处理
    public Connection getConnection() throws SQLException {
        //如果数据库的连接数大于0
        if (connList.size() > 0) {
            //从connList集合中取出一个数据库连接
            final Connection conn = connList.removeFirst();
            System.out.println(conn + "从数据库连接池中被取出，现在数据库连接池的大小是：" + connList.size());
            //返回Connection对象的代理对象
            return (Connection) Proxy.newProxyInstance(
                    JdbcPool.class.getClassLoader(),
                    new Class[]{Connection.class},
                    new InvocationHandler() {
                        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                            if (!method.getName().equals("close"))
                                return method.invoke(conn, args);
                            else {
                                //如果调用的是Connection对象的close方法，就把conn还给数据库连接池
                                connList.add(conn);
                                System.out.println(conn + "被还给数据库连接池，此时数据库连接池大小为：" + connList.size());
                                return null;
                            }
                        }
                    });
        } else
            throw new RuntimeException("对不起，数据库忙");
    }

    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    public void setLogWriter(PrintWriter arg0) throws SQLException {
    }

    public void setLoginTimeout(int arg0) throws SQLException {
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return false;
    }

    public <T> T unwrap(Class<T> cls) throws SQLException {
        return null;
    }

    public Connection getConnection(String arg0, String arg1) throws SQLException {
        return null;
    }
}
