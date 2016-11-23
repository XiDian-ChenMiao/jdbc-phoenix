package org.geotools.data.phoenix;

import com.vividsolutions.jts.geom.*;
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
import java.util.Map;
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
    protected Integer POINT = new Integer(2001);
    protected Integer GEOMETRY = new Integer(2007);
    /**
     * 创建索引的后缀名
     */
    private static String INDEX_SUFFIX = "_idx";

    protected PhoenixDialect(JDBCDataStore dataStore) {
        super(dataStore);
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
        if (POINT.equals(type))
            return "POINT";
        return super.getGeometryTypeName(type);
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
        return null;
    }

    /**
     * Decodes a geometry value from the result of a query.
     * <p>
     * This method is given direct access to a result set. The <tt>column</tt>
     * parameter is the index into the result set which contains the geometric
     * value.
     * </p>
     * <p>
     * An implementation should deserialize the value provided by the result
     * set into {@link Geometry} object. For example, consider an implementation
     * which deserializes from well known text:
     * <code>
     * <pre>
     *   String wkt = rs.getString( column );
     *   if ( wkt == null ) {
     *     return null;
     *   }
     *   return new WKTReader(factory).read( wkt );
     *   </pre>
     * </code>
     * Note that implementations must handle <code>null</code> values.
     * </p>
     * <p>
     * The <tt>factory</tt> parameter should be used to instantiate any geometry
     * objects.
     * </p>
     *
     * @param descriptor
     * @param rs
     * @param column
     * @param factory
     * @param cx
     */
    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column, GeometryFactory factory, Connection cx) throws IOException, SQLException {
        return null;
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

    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx) throws SQLException {
        return super.getMapping(columnMetaData, cx);
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
        /*使几何列非空，目的在于其上建立索引*/
        if (att instanceof GeometryDescriptor && !att.isNillable()) {
            sql.append(" NOT NULL");
            Class binding = att.getType().getBinding();
            if (isConcreteGeometry(binding))
                sql.append(" COMMENT '").append(binding.getSimpleName().toUpperCase()).append("'");
        }
    }

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
                    encodeColumnName(null, "f_table_schema", sql);/*添加表模式列*/
                    sql.append(" varchar(255), ");
                    encodeColumnName(null, "f_table_name", sql);/*添加表名列*/
                    sql.append(" varchar(255), ");
                    encodeColumnName(null, "f_geometry_column", sql);/*添加几何名称列*/
                    sql.append(" varchar(255), ");
                    encodeColumnName(null, "coord_dimension", sql);/*添加坐标维度列*/
                    sql.append(" int, ");
                    encodeColumnName(null, "srid", sql);/*添加空间参考ID列*/
                    sql.append(" int, ");
                    encodeColumnName(null, "type", sql);/*添加该列对应的类型列*/
                    sql.append(" varchar(32)");
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
            sql.append(" VALUES (");
            sql.append(schemaName != null ? "'" + schemaName + "'" : "NULL").append(", ");
            sql.append("'").append(featureType.getTypeName()).append("', ");
            sql.append("'").append(attributeDescriptor.getLocalName()).append("', ");
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

    @Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);
        mappings.put(Point.class, POINT);
        mappings.put(Geometry.class, GEOMETRY);
    }

    @Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        super.registerSqlTypeToClassMappings(mappings);
        mappings.put(POINT, Point.class);
        mappings.put(GEOMETRY, Geometry.class);
    }

    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);
        mappings.put("POINT", Point.class);
        mappings.put("GEOMETRY", Geometry.class);
    }

    /**
     * 判断类是否为正确几何图形绑定的类
     * @param binding
     * @return
     */
    boolean isConcreteGeometry( Class binding ) {
        return Point.class.isAssignableFrom(binding);
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
        return false;
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
