package org.geotools.data.phoenix;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

import java.io.IOException;
import java.util.Map;

/**
 * 文件描述：Phoenix的数据存储工厂
 * 创建作者：陈苗
 * 创建时间：2016/11/1 10:52
 */
public class PhoenixDataStoreFactory extends JDBCDataStoreFactory {
    /**
     * 数据库类型参数
     */
    public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "phoenix");
    /**
     * 默认对于Phoenix数据库连接的端口号
     */
    public static final Param PORT = new Param("port", Integer.class, "Port", true, 2181);
    /**
     * 默认的主机地址
     */
    public static final Param HOST = new Param("host", String.class, "Host", true, "localhost");
    /**
     * 默认的数据库名称
     */
    public static final Param DATABASE = new Param("database", String.class, "Database", false );

    @Override
    public String getDisplayName() {
        return "Phoenix";
    }

    /**
     * Returns a string to identify the type of the database.
     * <p>
     * Example: 'postgis'.
     * </p>
     */
    @Override
    protected String getDatabaseID() {
        return (String) DBTYPE.sample;
    }

    /**
     * Returns the fully qualified class name of the jdbc driver.
     * <p>
     * For example: org.postgresql.Driver
     * </p>
     */
    @Override
    protected String getDriverClassName() {
        return "org.apache.phoenix.jdbc.PhoenixDriver";
    }

    /**
     * Creates the dialect that the datastore uses for communication with the
     * underlying database.
     *
     * @param dataStore The datastore.
     */
    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
        return new PhoenixDialectBasic(dataStore);
    }

    /**
     * Override this to return a good validation query (a very quick one, such as one that
     * asks the database what time is it) or return null if the factory does not support
     * validation.
     *
     * @return
     */
    @Override
    protected String getValidationQuery() {
        return null;
    }

    /**
     * Describe the nature of the datasource constructed by this factory.
     * <p>
     * <p>
     * A non localized description of this data store type.
     * </p>
     *
     * @return A human readable description that is suitable for inclusion in a
     * list of available datasources.
     */
    @Override
    public String getDescription() {
        return "Phoenix Database";
    }

    /**
     * Get the jdbc url to connect phoenix database
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    protected String getJDBCUrl(Map params) throws IOException {
        String database = (String) DATABASE.lookUp(params);
        String host = (String) HOST.lookUp(params);
        if (host != null && !"".equals(host)) {
            Integer port = (Integer) PORT.lookUp(params);
            if (port != null)
                return "jdbc:phoenix:" + host + ":" + port + ":/" + database;
            else
                return "jdbc:phoenix:" + host + "/" + database;
        }
        /*如果未配置服务器的主机地址，则默认使用本机地址*/
        else
            return "jdbc:phoenix:127.0.0.1:/" + database;
    }

    /**
     * 设置访问数据库的参数配置
     * @param parameters Map of {@link Param} objects.
     */
    @Override
    protected void setupParameters(Map parameters) {
        parameters.put(PORT.key, PORT);
        parameters.put(DBTYPE.key, DBTYPE);
        parameters.put(DATABASE.key, DATABASE);
        parameters.put(HOST.key, HOST);
    }
}
