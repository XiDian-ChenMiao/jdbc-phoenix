package org.geotools.data.phoenix;

import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.JDBCJNDIDataStoreFactory;

import java.util.Map;

/**
 * 文件描述：针对Phoenix数据库的JNDI数据存储工厂
 * 创建作者：陈苗
 * 创建时间：2016/11/1 10:45
 */
public class PhoenixJNDIDataStoreFactory extends JDBCJNDIDataStoreFactory {

    /**
     * 传参构造器
     * @param delegate
     */
    protected PhoenixJNDIDataStoreFactory(JDBCDataStoreFactory delegate) {
        super(delegate);
    }

    /**
     * 无参构造器
     */
    public PhoenixJNDIDataStoreFactory() {
        super(new PhoenixDataStoreFactory());
    }

    @Override
    protected void setupParameters(Map parameters) {
        super.setupParameters(parameters);
        /*添加自定义关键字*/
    }
}
