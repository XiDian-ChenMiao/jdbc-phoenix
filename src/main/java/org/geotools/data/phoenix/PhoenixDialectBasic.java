package org.geotools.data.phoenix;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.Index;
import org.geotools.jdbc.JDBCDataStore;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * 文件描述：
 * 创建作者：陈苗
 * 创建时间：2016/11/7 11:39
 */
public class PhoenixDialectBasic extends BasicSQLDialect {
    PhoenixDialect delegate;

    public PhoenixDialectBasic(JDBCDataStore dataStore) {
        super(dataStore);
        this.delegate = new PhoenixDialect(dataStore);
    }

    @Override
    public void initializeConnection(Connection cx) throws SQLException {
        delegate.initializeConnection(cx);
    }

    @Override
    public boolean includeTable(String schemaName, String tableName, Connection cx) throws SQLException {
        return delegate.includeTable(schemaName, tableName, cx);
    }

    @Override
    public String getNameEscape() {
        return delegate.getNameEscape();
    }

    @Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        delegate.registerSqlTypeToClassMappings(mappings);
    }

    @Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        delegate.registerClassToSqlMappings(mappings);
    }

    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        delegate.registerSqlTypeNameToClassMappings(mappings);
    }

    @Override
    public void encodePostCreateTable(String tableName, StringBuffer sql) {
        delegate.encodePostCreateTable(tableName, sql);
    }

    @Override
    public void encodeTableName(String raw, StringBuffer sql) {
        delegate.encodeTableName(raw, sql);
    }

    @Override
    public void postCreateTable(String schemaName, SimpleFeatureType featureType, Connection cx) throws SQLException, IOException {
        delegate.postCreateTable(schemaName, featureType, cx);
    }

    @Override
    public void encodeColumnType(String sqlTypeName, StringBuffer sql) {
        delegate.encodeColumnType(sqlTypeName, sql);
    }

    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx) throws SQLException {
        return delegate.getMapping(columnMetaData, cx);
    }

    @Override
    public void postCreateFeatureType(SimpleFeatureType featureType, DatabaseMetaData metadata, String schemaName, Connection cx) throws SQLException {
        delegate.postCreateFeatureType(featureType, metadata, schemaName, cx);
    }

    @Override
    public void preDropTable(String schemaName, SimpleFeatureType featureType, Connection cx) throws SQLException {
        delegate.preDropTable(schemaName, featureType, cx);
    }

    @Override
    public void postDropTable(String schemaName, SimpleFeatureType featureType, Connection cx) throws SQLException {
        delegate.postDropTable(schemaName, featureType, cx);
    }

    @Override
    public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
        return delegate.getGeometrySRID(schemaName, tableName, columnName, cx);
    }

    @Override
    public int getGeometryDimension(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
        return delegate.getGeometryDimension(schemaName, tableName, columnName, cx);
    }

    @Override
    public String getGeometryTypeName(Integer type) {
        return delegate.getGeometryTypeName(type);
    }

    @Override
    public void encodePrimaryKey(String column, StringBuffer sql) {
        delegate.encodePrimaryKey(column, sql);
    }

    @Override
    public String getSequenceForColumn(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
        return delegate.getSequenceForColumn(schemaName, tableName, columnName, cx);
    }

    @Override
    public Object getNextSequenceValue(String schemaName, String sequenceName, Connection cx) throws SQLException {
        return delegate.getNextSequenceValue(schemaName, sequenceName, cx);
    }

    @Override
    public String encodeNextSequenceValue(String schemaName, String sequenceName) {
        return delegate.encodeNextSequenceValue(schemaName, sequenceName);
    }

    @Override
    public boolean lookupGeneratedValuesPostInsert() {
        return delegate.lookupGeneratedValuesPostInsert();
    }

    @Override
    public Object getNextAutoGeneratedValue(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
        return delegate.getNextAutoGeneratedValue(schemaName, tableName, columnName, cx);
    }

    @Override
    public Object getLastAutoGeneratedValue(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
        return delegate.getLastAutoGeneratedValue(schemaName, tableName, columnName, cx);
    }

    @Override
    public boolean isLimitOffsetSupported() {
        return delegate.isLimitOffsetSupported();
    }

    @Override
    public void applyLimitOffset(StringBuffer sql, int limit, int offset) {
        delegate.applyLimitOffset(sql, limit, offset);
    }

    @Override
    public FilterToSQL createFilterToSQL() {
        return new PhoenixFilterToSQL();
    }

    @Override
    protected boolean supportsSchemaForIndex() {
        return delegate.supportsSchemaForIndex();
    }

    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(Map<Integer, String> overrides) {
        delegate.registerSqlTypeToSqlTypeNameOverrides(overrides);
    }

    /**
     * 利用WKB将几何对象存储为数据库中的VARBINARY
     * @param value
     * @param dimension
     * @param srid
     * @param sql
     * @throws IOException
     */
    @Override
    public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
        if (value != null && !value.isEmpty()) {
            sql.append("'").append(new WKTWriter().write(value)).append("'");
        } else {
            sql.append("NULL");
        }
    }

    /**
     * 将数据库中存储的几何对象的二进制数据读取为集合对象时调用
     * @param descriptor
     * @param rs
     * @param column
     * @param factory
     * @param cx
     * @return
     * @throws IOException
     * @throws SQLException
     */
    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column, GeometryFactory factory, Connection cx) throws IOException, SQLException {
        byte[] bytes = rs.getBytes(column);
        if (bytes == null)
            return null;
        try {
            return new WKBReader(factory).read(bytes);
        } catch (ParseException e) {
            throw (IOException) new IOException("Error decoding wkb").initCause(e);
        }
    }

    /**
     * 编码一般值的时候调用
     * @param value
     * @param type
     * @param sql
     */
    @Override
    public void encodeValue(Object value, Class type, StringBuffer sql) {
        super.encodeValue(value, type, sql);
    }

    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        delegate.encodeGeometryEnvelope(tableName, geometryColumn, sql);
    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
        return delegate.decodeGeometryEnvelope(rs, column, cx);
    }

    @Override
    public boolean isAutoCommitQuery() {
        return delegate.isAutoCommitQuery();
    }

    @Override
    public void createIndex(Connection cx, SimpleFeatureType schema, String databaseSchema, Index index) throws SQLException {
        delegate.createIndex(cx, schema, databaseSchema, index);
    }

    @Override
    public void dropIndex(Connection cx, SimpleFeatureType schema, String databaseSchema, String indexName) throws SQLException {
        delegate.dropIndex(cx, schema, databaseSchema, indexName);
    }
}
