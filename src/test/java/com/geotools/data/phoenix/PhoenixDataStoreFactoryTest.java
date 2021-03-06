package com.geotools.data.phoenix;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import org.geotools.data.*;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.datasource.DBCPDataSourceFactory;
import org.geotools.data.jdbc.datasource.DataSourceFinder;
import org.geotools.data.phoenix.PhoenixDataStoreFactory;
import org.geotools.data.phoenix.util.GeoHashConverter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.AttributeImpl;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.geotools.feature.type.AttributeTypeImpl;
import org.geotools.feature.type.GeometryDescriptorImpl;
import org.geotools.feature.type.GeometryTypeImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.jdbc.Index;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.PrimaryKeyFinder;
import org.junit.Test;
import org.opengis.feature.Attribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.*;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.expression.Expression;

import java.io.IOException;
import java.io.StringWriter;
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
        SimpleFeatureSource simpleFeaturerSource = jdbcDataStore.getFeatureSource("geotools_cm");

        Transaction transaction = new DefaultTransaction();
        SimpleFeatureStore simpleFeatureStore = (SimpleFeatureStore) simpleFeaturerSource;


        SimpleFeatureType schema = simpleFeatureStore.getSchema();/*获取表结构信息*/
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(schema);/*获取要素构建器*/
        List<SimpleFeature> infos = new ArrayList<>();

        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);

        infos.add(builder.buildFeature("3", new Object[]{geometryFactory.createPoint(new Coordinate(3, 3)), 3, "3"}));
        infos.add(builder.buildFeature("4", new Object[]{geometryFactory.createPoint(new Coordinate(4, 4)), 4, "4"}));

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
        SimpleFeatureSource featureSource = dataStore.getFeatureSource("geotools_cm");
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
        SimpleFeatureType type = jdbcDataStore.getSchema("geotools_cm");
        FilterFactory filterFactory = CommonFactoryFinder.getFilterFactory(null);
        Expression literal = filterFactory.literal(1);
        Expression prop = filterFactory.property("INTPROPERRTY");
        PropertyIsEqualTo filter = filterFactory.equals(prop, literal);
        StringWriter buffer = new StringWriter();
        FilterToSQL filterToSQL = new FilterToSQL(buffer);
        filterToSQL.setFeatureType(type);
        filterToSQL.encode(filter);
        Query query = new Query("geotools_cm", filter);
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
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
        GeometryType geo_attr = new GeometryTypeImpl(new NameImpl("geometry"), Point.class, null, false, false, Collections.EMPTY_LIST, null, null);
        GeometryDescriptor geo_desc = new GeometryDescriptorImpl(geo_attr, new NameImpl("geometry"), 0, 2, false, geometryFactory.createPoint(new Coordinate(0, 0)));

        typeBuilder.add(geo_desc);
        typeBuilder.add("intProperty", Integer.class);
        typeBuilder.add("stringProperty", String.class);
        typeBuilder.setName("geotools_cm");

        SimpleFeatureType simpleFeatureType = typeBuilder.buildFeatureType();
        JDBCDataStore jdbcDataStore = (JDBCDataStore) DataStoreFinder.getDataStore(params);
        jdbcDataStore.createSchema(simpleFeatureType);
    }

    @Test
    public void testCreateIndex() throws IOException {
        JDBCDataStore jdbcDataStore = (JDBCDataStore) DataStoreFinder.getDataStore(params);
        Index geoHashIndex = new Index("geotools_cm", "GEOHASH_IDX", false, "geohash");
        jdbcDataStore.createIndex(geoHashIndex);
    }

    @Test
    public void testDropIndex() throws IOException {
        JDBCDataStore jdbcDataStore = (JDBCDataStore) DataStoreFinder.getDataStore(params);
        jdbcDataStore.dropIndex("geotools_cm", "GEOHASH_IDX");
    }

    /**
     * 测试将GEOHASH值转为坐标值的算法
     */
    @Test
    public void testGeohashToCoordinate() {
        double[] coordinate = GeoHashConverter.geohashToLongAndLati(-4604394606633189360L);
        System.out.println(Arrays.toString(coordinate));
    }

    public static void main(String[] args) throws IOException {
        PhoenixDataStoreFactoryTest test = new PhoenixDataStoreFactoryTest();
        test.testCreateTable();
    }
}
