package com.geotools.data.phoenix;

import com.vividsolutions.jts.geom.*;
import org.geotools.data.*;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.datasource.DBCPDataSourceFactory;
import org.geotools.data.jdbc.datasource.DataSourceFinder;
import org.geotools.data.phoenix.PhoenixDataStoreFactory;
import org.geotools.data.phoenix.PhoenixDialectBasic;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureComparators;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.jdbc.*;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.identity.FeatureId;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * 文件描述：Phoenix数据库的简单测试类
 * 创建作者：陈苗
 * 创建时间：2016/11/12 20:49
 */
public class PhoenixDataStoreFactoryTest {
    private static Map<String, Object> params = new HashMap<>();

    static {
        params.put(PhoenixDataStoreFactory.DATABASE.key, "hbase-unsecure");
        params.put(PhoenixDataStoreFactory.DBTYPE.key, "phoenix");
        params.put(PhoenixDataStoreFactory.HOST.key, "192.168.3.201");
        params.put(PhoenixDataStoreFactory.PORT.key, 2181);
        params.put(PhoenixDataStoreFactory.USER.key, "root");
        params.put(PhoenixDataStoreFactory.PASSWD.key, "root");
    }

    @Test
    public void testFactory() throws IOException, SQLException {
        PhoenixDataStoreFactory factory = new PhoenixDataStoreFactory();
        if (factory.canProcess(params)) {
            DataStore dataStore = factory.createDataStore(params);
            JDBCDataStore jdbcDataStore = (JDBCDataStore) dataStore;
            if (dataStore != null) {
                String[] tableNames = dataStore.getTypeNames();
                PrimaryKeyFinder finder = jdbcDataStore.getPrimaryKeyFinder();
                if (finder != null) {
                    int index = 1;
                    for (String tableName : tableNames) {
                        System.out.println(index++ + "\t" + tableName);
                    }
                }
            }
        } else {
            System.out.println("Configuration parse failed.");
        }
    }

    @Test
    public void testFactorySpi() throws IOException {
        if (new DBCPDataSourceFactory().isAvailable()) {
            System.out.println("isAvailable");
        }
        DataSourceFinder.scanForPlugins();
        DataStore dataStore = DataStoreFinder.getDataStore(params);
        if (dataStore == null) {
            System.out.println("Register PhoenixDataStoreFactory Failed.");
        }
    }

    @Test
    public void testAddFeatures() throws IOException {
        PhoenixDataStoreFactory factory = new PhoenixDataStoreFactory();
        DataStore dataStore = factory.createDataStore(params);
        JDBCDataStore jdbcDataStore = (JDBCDataStore) dataStore;
        SimpleFeatureSource simpleFeaturerSource = jdbcDataStore.getFeatureSource("GEOTOOLS_CM");

        Transaction transaction = new DefaultTransaction();
        SimpleFeatureStore simpleFeatureStore = (SimpleFeatureStore) simpleFeaturerSource;


        SimpleFeatureType schema = simpleFeatureStore.getSchema();/*获取表结构信息*/
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(schema);/*获取要素构建器*/
        List<SimpleFeature> infos = new ArrayList<>();

        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);

        infos.add(builder.buildFeature("1", new Object[]{geometryFactory.createPoint(new Coordinate(1, 1)), 1, "1"}));
        infos.add(builder.buildFeature("2", new Object[]{geometryFactory.createPoint(new Coordinate(2, 2)), 2, "2"}));

        SimpleFeatureCollection collection = new ListFeatureCollection(schema, infos);

        try {
            simpleFeatureStore.addFeatures(collection);
            transaction.commit();
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Test
    public void testSearch() throws IOException, CQLException {
        DataStore dataStore = DataStoreFinder.getDataStore(params);
        SimpleFeatureSource featureSource = dataStore.getFeatureSource("GEOTOOLS_CM");
        Filter filter = CQL.toFilter("INTPROPERTY = 1");

        SimpleFeatureCollection features = featureSource.getFeatures(filter);


        System.out.println("Found:" + features.size() + " features.");
        SimpleFeatureIterator iterator = features.features();
        try {
            while (iterator.hasNext()) {

                SimpleFeature feature = iterator.next();
                System.out.println(feature.getID());
                for (Property property : feature.getProperties()) {
                    System.out.println("\t" + property.getName() + " = " + property.getValue());
                }
            }
        } catch (Exception e) {
            iterator.close();
        }
    }

    @Test
    public void testQueryByProperty() throws Exception {
        JDBCDataStore jdbcDataStore = (JDBCDataStore) DataStoreFinder.getDataStore(params);
        SimpleFeatureType type = jdbcDataStore.getSchema("GEOTOOLS_CM");
        FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);
        Expression literal = filterFactory.literal(1);
        Expression prop = filterFactory.property("INTPROPERRTY");
        PropertyIsEqualTo filter= filterFactory.equals(prop, literal);
        StringWriter buffer = new StringWriter();
        FilterToSQL filterToSQL = new FilterToSQL(buffer);
        filterToSQL.setFeatureType(type);
        filterToSQL.encode(filter);
        Query query = new Query("GEOTOOLS_CM", filter);
        FeatureReader<SimpleFeatureType, SimpleFeature> reader = jdbcDataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        if (reader != null) {
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                List<Object> attributes = feature.getAttributes();
                for (Object obj : attributes)
                    System.out.print(obj.toString() + "\t");
                System.out.println("\n");
            }
        }
    }

    @Test
    public void testCreateTable() throws IOException {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.add("geometry", Point.class, 4326);
        typeBuilder.add("intProperty", Integer.class);
        typeBuilder.add("stringProperty", String.class);
        typeBuilder.setName("GEOTOOLS_CM");

        SimpleFeatureType simpleFeatureType = typeBuilder.buildFeatureType();
        JDBCDataStore jdbcDataStore = (JDBCDataStore) DataStoreFinder.getDataStore(params);
        jdbcDataStore.createSchema(simpleFeatureType);
    }

    public static void main(String[] args) throws IOException, CQLException {
        PhoenixDataStoreFactoryTest test = new PhoenixDataStoreFactoryTest();
        test.testSearch();
    }
}
