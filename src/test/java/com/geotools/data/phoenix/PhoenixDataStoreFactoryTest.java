package com.geotools.data.phoenix;

import com.google.common.collect.Iterators;
import junit.framework.TestCase;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.jdbc.JDBCUtils;
import org.geotools.data.phoenix.PhoenixDataStoreFactory;
import org.geotools.data.phoenix.PhoenixDialectBasic;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 文件描述：Phoenix数据库的简单测试类
 * 创建作者：陈苗
 * 创建时间：2016/11/12 20:49
 */
public class PhoenixDataStoreFactoryTest extends TestCase {
    @Test
    public void testFactory() throws IOException {
        PhoenixDataStoreFactory factory = new PhoenixDataStoreFactory();
        Map<String, Object> params = new HashMap<>();

        params.put(PhoenixDataStoreFactory.DATABASE.key, "hbase-unsecure");
        params.put(PhoenixDataStoreFactory.DBTYPE.key, "phoenix");
        params.put(PhoenixDataStoreFactory.HOST.key, "192.168.3.201");
        params.put(PhoenixDataStoreFactory.PORT.key, 2181);
        params.put(JDBCDataStoreFactory.USER.key, "root");
        params.put(JDBCDataStoreFactory.PASSWD.key, "root");
        if (factory.canProcess(params)) {
            DataStore dataStore = factory.createDataStore(params);
            if (dataStore != null) {
                PhoenixDialectBasic dialectBasic = new PhoenixDialectBasic((JDBCDataStore) dataStore);
                if (dialectBasic != null) {

                }
            }
        }
    }
    @Test
    public void testFactorySpi() {
        Iterator<PhoenixDataStoreFactory> filtered = Iterators.filter(
                DataStoreFinder.getAvailableDataStores(), PhoenixDataStoreFactory.class
        );
        assertTrue(filtered.hasNext());
        assertTrue(filtered.next() instanceof PhoenixDataStoreFactory);
    }
}
