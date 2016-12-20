package org.geotools.data.phoenix;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.Geometries;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.SQLDialect;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

/**
 * 文件描述：Phoenix方言类
 * 创建作者：陈苗
 * 创建时间：2016/11/1 10:44
 */
public class PhoenixDialect extends SQLDialect {
    /**
     * Phoenix的空间类型
     */
    protected Integer POINT = new Integer(3001);
    protected Integer MULTIPOINT = new Integer(3002);
    protected Integer LINESTRING = new Integer(3003);
    protected Integer MULTILINESTRING = new Integer(3004);
    protected Integer POLYGON = new Integer(3005);
    protected Integer MULTIPOLYGON = new Integer(3006);
    protected Integer GEOMETRY = new Integer(3007);

    /**
     * 几何类型名称与类型的映射
     */
    protected final static Map<String, Class> TYPE_TO_CLASS_MAP = new HashMap<String, Class>() {
        {
            put("POINT", Point.class);
            put("LINESTRING", LineString.class);
            put("POLYGON", Polygon.class);
            put("MULTIPOINT", MultiPoint.class);
            put("MULTILINESTRING", MultiLineString.class);
            put("MULTIPOLYGON", MultiPolygon.class);
            put("GEOMETRY", Geometry.class);
            put("GEOMETRYCOLLETION", GeometryCollection.class);
        }
    };
    /**
     * 创建索引的后缀名
     */
    private static String INDEX_SUFFIX = "_idx";
    /**
     * 设置是否一次插入多次读取标志
     */
    protected boolean isImmutableRows;

    protected PhoenixDialect(JDBCDataStore dataStore) {
        super(dataStore);
        isImmutableRows = true;/*默认设置为只能一次插入多次读取*/
    }

    public void setImmutableRows(boolean immutableRows) {
        isImmutableRows = immutableRows;
    }

    public boolean isImmutableRows() {
        return isImmutableRows;
    }

    /**
     * 设置索引后缀名
     * @param indexSuffix
     */
    public void setIndexSuffix(String indexSuffix) {
        INDEX_SUFFIX = indexSuffix;
    }

    public String getNameEscape() {
        return "";
    }

    /**
     * 通过类型编号获取几何类型名称
     * @param type
     * @return
     */
    @Override
    public String getGeometryTypeName(Integer type) {
        if (POINT.equals(type)) {
            return "POINT";
        }

        if (MULTIPOINT.equals(type)) {
            return "MULTIPOINT";
        }

        if (LINESTRING.equals(type)) {
            return "LINESTRING";
        }

        if (MULTILINESTRING.equals(type)) {
            return "MULTILINESTRING";
        }

        if (POLYGON.equals(type)) {
            return "POLYGON";
        }

        if (MULTIPOLYGON.equals(type)) {
            return "MULTIPOLYGON";
        }

        if (GEOMETRY.equals(type)) {
            return "GEOMETRY";
        }

        return super.getGeometryTypeName(type);
    }

    /**
     * 编码表名，如果此时已形成的SQL中带有插入关键字，则应该将通用的INSERT替换为UPSERT关键字
     * @param raw
     * @param sql
     */
    @Override
    public void encodeTableName(String raw, StringBuffer sql) {
        if (sql.toString().toUpperCase().indexOf("INSERT") >= 0) {
            int startIndex = sql.toString().toUpperCase().indexOf("INSERT");
            sql.replace(startIndex, startIndex + 6, "UPSERT");
        }
        super.encodeTableName(raw, sql);
    }

    /**
     * 建表之后的表选项参数设置
     * @param tableName
     * @param sql
     */
    @Override
    public void encodePostCreateTable(String tableName, StringBuffer sql) {
        if (isImmutableRows) {
            sql.append("IMMUTABLE_ROWS = true");
        }
    }

    /**
     * Encodes the name of a geometry column in a SELECT statement.
     * <p>
     * This method should wrap the column name in any functions that are used to
     * retrieve its value. For instance, often it is necessary to use the function
     * <code>asText</code>, or <code>asWKB</code> when fetching a geometry.
     * </p>
     * <p>
     * This method must also be sure to properly encode the name of the column with
     * the {@link #encodeColumnName(String, String, StringBuffer)} function.
     * </p>
     * <p>
     * Example:
     * </p>
     * <p>
     * <pre>
     *   <code>
     *   sql.append( "asText(" );
     *   column( gatt.getLocalName(), sql );
     *   sql.append( ")" );
     *   </code>
     * </pre>
     * <p>
     * </p>
     * <p>
     * This default implementation calls the deprecated
     * {@link #encodeGeometryColumn(GeometryDescriptor, String, int, StringBuffer)} version of
     * this method for backward compatibility reasons.
     * </p>
     */
    @Override
    public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix, int srid, Hints hints, StringBuffer sql) {
        sql.append("asWKB(");
        encodeColumnName(prefix, gatt.getLocalName(), sql);
        sql.append(")");
    }

    /**
     * Encodes the spatial extent function of a geometry column in a SELECT statement.
     * <p>
     * This method must also be sure to properly encode the name of the column
     * with the {@link #encodeColumnName(String, StringBuffer)} function.
     * </p>
     *
     * @param tableName
     * @param geometryColumn
     * @param sql
     */
    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        sql.append("asWKB(");
        sql.append("envelope(");
        encodeColumnName(null, geometryColumn, sql);
        sql.append("))");
    }

    /**
     * Decodes the result of a spatial extent function in a SELECT statement.
     * <p>
     * This method is given direct access to a result set. The <tt>column</tt>
     * parameter is the index into the result set which contains the spatial
     * extent value. The query for this value is build with the {@link #encodeGeometryEnvelope(String, String, StringBuffer)}
     * method.
     * </p>
     * <p>
     * This method must not read any other objects from the result set other then
     * the one referenced by <tt>column</tt>.
     * </p>
     *
     * @param rs     A result set
     * @param column Index into the result set which points at the spatial extent
     *               value.
     * @param cx
     */
    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
        byte[] wkb = rs.getBytes(column);
        try {
            Polygon polygon = (Polygon) new WKBReader().read(wkb);

            return polygon.getEnvelopeInternal();
        } catch (ParseException e) {
            String msg = "Error decoding wkb for envelope";
            throw (IOException) new IOException(msg).initCause(e);
        }
    }

    /**
     * 将数据库空间列中取出的二进制流利用WKB读取成为一个空间对象
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
        if (bytes == null) {
            return null;
        }
        try {
            return new WKBReader(factory).read(bytes);
        } catch (ParseException e) {
            String msg = "Error decoding wkb";
            throw (IOException) new IOException(msg).initCause(e);
        }
    }

    @Override
    public void initializeConnection(Connection cx) throws SQLException {
        super.initializeConnection(cx);
    }

    /**
     * 判断是否在datastore中已经包含了此表
     * @param schemaName The schema of the table, might be <code>null</code>..
     * @param tableName The name of the table.
     * @param cx Database connection.
     * @return
     * @throws SQLException
     */
    @Override
    public boolean includeTable(String schemaName, String tableName, Connection cx) throws SQLException {
        if ("geometry_columns".equalsIgnoreCase(tableName)) {
            return false;
        }
        return super.includeTable(schemaName, tableName, cx);
    }

    /**
     * 获取映射对象
     * @param columnMetaData
     * @param cx
     * @return
     * @throws SQLException
     */
    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx) throws SQLException {
        String typeName = columnMetaData.getString("TYPE_NAME");
        if (typeName != null && "VARBINARY".equalsIgnoreCase(typeName)) {
            String gType = lookupGeometryType(columnMetaData, cx, "geometry_columns", "f_geometry_column");
            if (gType != null && TYPE_TO_CLASS_MAP.containsKey(gType))
                return TYPE_TO_CLASS_MAP.get(gType);
        }
        return super.getMapping(columnMetaData, cx);
    }

    /**
     * 从元数据表中获取指定空间列对应的列类型
     * @param columnMetaData
     * @param cx
     * @param gTableName
     * @param gColumnName
     * @return
     */
    private String lookupGeometryType(ResultSet columnMetaData, Connection cx, String gTableName, String gColumnName) throws SQLException {
        String tableName = columnMetaData.getString("TABLE_NAME");
        String columnName = columnMetaData.getString("COLUMN_NAME");
        String schemaName = columnMetaData.getString("TABLE_SCHEM");
        Statement statement = null;
        ResultSet result = null;
        try {
            StringBuffer sql = new StringBuffer();
            sql.append("SELECT type FROM ").append(gTableName.toUpperCase()).append(" WHERE 1 = 1 ");
            if (schemaName != null && !"".equals(schemaName))
                sql.append("AND f_table_schema = '").append(schemaName.toUpperCase()).append("' ");
            sql.append("AND f_table_name = '").append(tableName.toUpperCase()).append("' ");
            sql.append("AND ").append(gColumnName).append(" = '").append(columnName.toUpperCase()).append("'");
            LOGGER.log(Level.FINE, "Geometry type check; {0}", sql);
            statement = cx.createStatement();
            result = statement.executeQuery(sql.toString());
            if (result.next()) {
                return result.getString(1);
            }
        } finally {
            dataStore.closeSafe(result);
            dataStore.closeSafe(statement);
        }
        return null;
    }

    /**
     * 编码主键
     * @param column
     * @param sql
     */
    @Override
    public void encodePrimaryKey(String column, StringBuffer sql) {
        encodeColumnName(null, column, sql);
        sql.append(" INTEGER PRIMARY KEY ASC");
    }

    @Override
    public boolean lookupGeneratedValuesPostInsert() {
        return true;
    }

    /**
     * 在创建表中列之后执行的函数
     * @param att The attribute corresponding to the column.
     * @param sql
     */
    @Override
    public void encodePostColumnCreateTable(AttributeDescriptor att, StringBuffer sql) {
        super.encodePostColumnCreateTable(att, sql);
        /*将凡是空间类型修饰的属性统一处理为VARBINARY*/
        if (att instanceof GeometryDescriptor) {
            int lastBracketIndex = sql.lastIndexOf(" ");/*最后一个空格的位置*/
            sql.setLength(lastBracketIndex + 1);
            sql.append("VARBINARY");
        }
        /*使几何列非空，目的在于其上建立索引*/
        if (att instanceof GeometryDescriptor && !att.isNillable()) {
            sql.append(" NOT NULL");
            Class binding = att.getType().getBinding();
            if (isConcreteGeometry(binding))
                sql.append(" COMMENT '").append(binding.getSimpleName().toUpperCase()).append("'");
        }
    }

    /**
     * 当创建表之后附带创建存放表中空间列信息的元数据表
     * @param schemaName  The name of the schema, may be <code>null</code>.
     * @param featureType The feature type that has just been created on the database.
     * @param cx          Database connection.
     * @throws SQLException
     * @throws IOException
     */
    @Override
    public void postCreateTable(String schemaName, SimpleFeatureType featureType, Connection cx) throws SQLException, IOException {
        super.postCreateTable(schemaName, featureType, cx);
        DatabaseMetaData md = cx.getMetaData();
        ResultSet rs = md.getTables(null, dataStore.escapeNamePattern(md, schemaName),
                dataStore.escapeNamePattern(md, "geometry_columns"), new String[]{"TABLE"});
        try {
            if (!rs.next()) {
                Statement st = cx.createStatement();
                try {
                    StringBuffer sql = new StringBuffer("CREATE TABLE ");
                    encodeTableName("geometry_columns", sql);
                    sql.append("(");
                    encodeColumnName(null, "id", sql);
                    sql.append(" VARCHAR(200) NOT NULL PRIMARY KEY, ");
                    encodeColumnName(null, "f_table_schema", sql);/*添加表模式列*/
                    sql.append(" VARCHAR(255), ");
                    encodeColumnName(null, "f_table_name", sql);/*添加表名列*/
                    sql.append(" VARCHAR(255), ");
                    encodeColumnName(null, "f_geometry_column", sql);/*添加几何名称列*/
                    sql.append(" VARCHAR(255), ");
                    encodeColumnName(null, "coord_dimension", sql);/*添加坐标维度列*/
                    sql.append(" INTEGER, ");
                    encodeColumnName(null, "srid", sql);/*添加空间参考ID列*/
                    sql.append(" INTEGER, ");
                    encodeColumnName(null, "type", sql);/*添加该列对应的类型列*/
                    sql.append(" VARCHAR(32)");
                    sql.append(")");
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine(sql.toString());
                    }
                    st.execute(sql.toString());
                } finally {
                    dataStore.closeSafe(st);
                }
            }
        } finally {
            dataStore.closeSafe(rs);
        }

        for (AttributeDescriptor attributeDescriptor : featureType.getAttributeDescriptors()) {
            if (!(attributeDescriptor instanceof GeometryDescriptor))
                continue;
            GeometryDescriptor gd = (GeometryDescriptor) attributeDescriptor;
            if (!attributeDescriptor.isNillable()) {
                StringBuffer sql = new StringBuffer("CREATE INDEX ");
                encodeColumnName(null, gd.getLocalName() + INDEX_SUFFIX, sql);
                sql.append(" ON ");
                sql.append(schemaName == null ? "" : schemaName + ".");
                encodeTableName(featureType.getTypeName(), sql);
                sql.append("(");
                encodeColumnName(null, gd.getLocalName(), sql);
                sql.append(")");

                LOGGER.fine(sql.toString());
                Statement statement = cx.createStatement();
                try {
                    statement.execute(sql.toString());
                } finally {
                    dataStore.closeSafe(statement);
                }
            }
            CoordinateReferenceSystem crs = gd.getCoordinateReferenceSystem();
            int srid = -1;
            if (crs != null) {
                Integer i = null;
                try {
                    i = CRS.lookupEpsgCode(crs, true);
                } catch (FactoryException e) {
                    LOGGER.log(Level.FINER, "Could not determine epsg code", e);
                }
                srid = i != null ? i : srid;
            }
            StringBuffer sql = new StringBuffer("UPSERT INTO ");
            encodeTableName("geometry_columns", sql);
            sql.append(" VALUES (").append("'").append(UUID.randomUUID().toString().replace("-", "")).append("', ");
            sql.append(schemaName != null ? "'" + schemaName.toUpperCase() + "'" : "NULL").append(", ");
            sql.append("'").append(featureType.getTypeName().toUpperCase()).append("', ");
            sql.append("'").append(attributeDescriptor.getLocalName().toUpperCase()).append("', ");
            sql.append("2, ");
            sql.append(srid).append(", ");
            Geometries g = Geometries.getForBinding((Class<? extends Geometry>) gd.getType().getBinding());
            sql.append("'").append(g != null ? g.getName().toUpperCase() : "GEOMETRY").append("')");
            LOGGER.fine(sql.toString());
            Statement st = cx.createStatement();
            try {
                st.execute(sql.toString());
            } finally {
                dataStore.closeSafe(st);
            }
        }
    }

    /**
     * 从元数据表中获取空间参考ID
     * @param schemaName The database schema, could be <code>null</code>.
     * @param tableName  The table, never <code>null</code>.
     * @param columnName The column name, never <code>null</code>
     * @param cx         The database connection.
     * @return
     * @throws SQLException
     */
    @Override
    public Integer getGeometrySRID(String schemaName, String tableName, String columnName, Connection cx) throws SQLException {
        /*形成的SQL:
        select srid
        from geometry_columns
        where f_table_schema (IS NULL | = '模式名')
        AND f_table_name = '表名' AND f_geometry_column = '列名'*/
        /*在执行此方法之前，先需检查是否存在geometry_columns表*/
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT ");
        encodeColumnName(null, "srid", sql);
        sql.append(" FROM ");
        encodeTableName("geometry_columns", sql);
        sql.append(" WHERE ");

        encodeColumnName(null, "f_table_schema", sql);

        if (schemaName != null) {
            sql.append(" = '").append(schemaName).append("'");
        } else {
            sql.append(" IS NULL");
        }
        sql.append(" AND ");

        encodeColumnName(null, "f_table_name", sql);
        sql.append(" = '").append(tableName).append("' AND ");

        encodeColumnName(null, "f_geometry_column", sql);
        sql.append(" = '").append(columnName).append("'");

        dataStore.getLogger().fine(sql.toString());

        Statement st = cx.createStatement();
        try {
            ResultSet rs = st.executeQuery(sql.toString());
            try {
                if (rs.next()) {
                    return new Integer(rs.getInt(1));
                }
            } finally {
                dataStore.closeSafe(rs);
            }
        } catch (SQLException e) {
            /*geometry_columns不存在时的处理方法*/
        } finally {
            dataStore.closeSafe(st);
        }
        return null;
    }

    /**
     * 注册空间数据类型与数据库中类型数字字段的映射
     * @param mappings
     */
    @Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);
        mappings.put(Point.class, POINT);
        mappings.put(LineString.class, LINESTRING);
        mappings.put(Polygon.class, POLYGON);
        mappings.put(MultiPoint.class, MULTIPOINT);
        mappings.put(MultiLineString.class, MULTILINESTRING);
        mappings.put(MultiPolygon.class, MULTIPOLYGON);
        mappings.put(Geometry.class, GEOMETRY);
    }

    /**
     * 注册数据库中类型数字字段与空间数据类型的映射
     * @param mappings
     */
    @Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        super.registerSqlTypeToClassMappings(mappings);
        mappings.put(POINT, Point.class);
        mappings.put(LINESTRING, LineString.class);
        mappings.put(POLYGON, Polygon.class);
        mappings.put(MULTIPOINT, MultiPoint.class);
        mappings.put(MULTILINESTRING, MultiLineString.class);
        mappings.put(MULTIPOLYGON, MultiPolygon.class);
        mappings.put(GEOMETRY, Geometry.class);
    }

    /**
     * 注册空间类型字面量与空间数据类型的映射
     * @param mappings
     */
    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);
        mappings.put("POINT", Point.class);
        mappings.put("LINESTRING", LineString.class);
        mappings.put("POLYGON", Polygon.class);
        mappings.put("MULTIPOINT", MultiPoint.class);
        mappings.put("MULTILINESTRING", MultiLineString.class);
        mappings.put("MULTIPOLYGON", MultiPolygon.class);
        mappings.put("GEOMETRY", Geometry.class);
        mappings.put("GEOMETRYCOLLETION", GeometryCollection.class);
    }

    /**
     * 数据库类型数字值与名称映射的注册
     * @param overrides
     */
    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(Map<Integer, String> overrides) {
        super.registerSqlTypeToSqlTypeNameOverrides(overrides);
        overrides.put(POINT, "POINT");
        overrides.put(LINESTRING, "LINESTRING");
        overrides.put(POLYGON, "POLYGON");
        overrides.put(MULTIPOINT, "MULTIPOINT");
        overrides.put(MULTILINESTRING, "MULTILINESTRING");
        overrides.put(MULTIPOLYGON, "MULTIPOLYGON");
        overrides.put(GEOMETRY, "GEOMETRY");

        overrides.put(Types.INTEGER, "INTEGER");
        overrides.put(Types.BIGINT, "BIGINT");
        overrides.put(Types.DOUBLE, "DOUBLE");
        overrides.put(Types.VARCHAR, "VARCHAR");
        overrides.put(Types.BINARY, "BINARY");
        overrides.put(Types.VARBINARY, "VARBINARY");
        overrides.put(Types.CHAR, "CHAR");
        overrides.put(Types.DATE, "DATE");
        overrides.put(Types.TIME, "TIME");
        overrides.put(Types.BOOLEAN, "BOOLEAN");
        overrides.put(Types.FLOAT, "FLOAT");
        overrides.put(Types.DECIMAL, "DECIMAL");
        overrides.put(Types.TIMESTAMP, "TIMESTAMP");
    }

    /**
     * 判断类是否为正确几何图形绑定的类
     * @param binding
     * @return
     */
    boolean isConcreteGeometry( Class binding ) {
        return Geometry.class.isAssignableFrom(binding);
    }

    @Override
    protected boolean supportsSchemaForIndex() {
        return true;
    }

    /**
     * 默认不支持分页查询
     * @return
     */
    @Override
    public boolean isLimitOffsetSupported() {
        return true;
    }

    /**
     * 拼接分页SQL
     * @param sql
     * @param limit
     * @param offset
     */
    @Override
    public void applyLimitOffset(StringBuffer sql, int limit, int offset) {
        if (limit >= 0 && limit < Integer.MAX_VALUE) {
            if (offset > 0)
                sql.append(" LIMIT " + limit + " OFFSET " + offset);
            else
                sql.append(" LIMIT " + limit + " OFFSET 0");
        } else if (offset > 0) {
            sql.append(" LIMIT " + Long.MAX_VALUE + " OFFSET " + offset);
        }
    }

    /**
     * 删除索引
     * @param cx
     * @param schema
     * @param databaseSchema
     * @param indexName
     * @throws SQLException
     */
    @Override
    public void dropIndex(Connection cx, SimpleFeatureType schema, String databaseSchema, String indexName) throws SQLException {
        StringBuffer sql = new StringBuffer();
        String escape = getNameEscape();
        sql.append("DROP INDEX ");
        sql.append(escape).append(indexName).append(escape).append(" ON ");
        if (databaseSchema != null) {
            encodeSchemaName(databaseSchema, sql);
            sql.append(".");
        }
        encodeTableName(schema.getTypeName(), sql);
        Statement statement;
        try {
            statement = cx.createStatement();
            statement.execute(sql.toString());
            if (!cx.getAutoCommit()) {
                cx.commit();
            }
        } finally {
            dataStore.closeSafe(cx);
        }
    }
}
