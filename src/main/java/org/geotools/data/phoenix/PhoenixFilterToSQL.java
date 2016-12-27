package org.geotools.data.phoenix;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.*;

import java.io.IOException;

import static org.geotools.filter.FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS;

/**
 * 文件描述：Phoenix specific filter encoder.
 * 创建作者：陈苗
 * 创建时间：2016/11/1 10:47
 */
public class PhoenixFilterToSQL extends FilterToSQL {
    /**
     * 添加Phoenix所支持的空间函数
     * @return
     */
    @Override
    protected FilterCapabilities createFilterCapabilities() {
        /*添加函数*/
        FilterCapabilities caps = super.createFilterCapabilities();
        caps.addType(BBOX.class);
        caps.addType(Contains.class);
        caps.addType(Disjoint.class);
        caps.addType(Crosses.class);
        caps.addType(Equals.class);
        caps.addType(Intersects.class);
        caps.addType(Overlaps.class);
        caps.addType(Touches.class);
        caps.addType(Within.class);
        caps.addType(DWithin.class);
        caps.addType(Beyond.class);
        return caps;
    }

    @Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
        Geometry g = (Geometry) evaluateLiteral(expression, Geometry.class);
        if (g instanceof LinearRing)
            g = g.getFactory().createLineString(((LinearRing) g).getCoordinateSequence());
        out.write("ST_GEOFROMTEXT('" + g.toText() + "', " + currentSRID + ")");
    }

    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, PropertyName property, Literal geometry, boolean swapped, Object extraData) {
        return visitBinarySpatialOperator(filter, (Expression) property, (Expression) geometry, swapped, extraData);
    }

    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1, Expression e2, Object extraData) {
        return visitBinarySpatialOperator(filter, e1, e2, false, extraData);
    }

    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1,
                                                Expression e2, boolean swapped, Object extraData) {
        try {
            if (!(filter instanceof Disjoint)) {
                out.write("ST_MBRINTERSECTS(");
                e1.accept(this, extraData);
                out.write(",");
                e2.accept(this, extraData);
                out.write(")");

                if (!(filter instanceof BBOX))
                    out.write(" AND ");
            }
            if (filter instanceof BBOX)
                return extraData;
            if (filter instanceof DistanceBufferOperator) {
                out.write("ST_DISTANCE(");
                e1.accept(this, extraData);
                out.write(", ");
                e2.accept(this, extraData);
                out.write(")");
                if (filter instanceof DWithin) {
                    out.write("<");
                } else if (filter instanceof Beyond) {
                    out.write(">");
                } else {
                    throw new RuntimeException("Unknown distance operator");
                }
                out.write(Double.toString(((DistanceBufferOperator) filter).getDistance()));
            } else {
                if (filter instanceof Contains) {
                    out.write("ST_CONTAINS(");
                } else if (filter instanceof Crosses) {
                    out.write("ST_CROSSES(");
                } else if (filter instanceof Disjoint) {
                    out.write("ST_DISJOINT(");
                } else if (filter instanceof Equals) {
                    out.write("ST_EQUALS(");
                } else if (filter instanceof Intersects) {
                    out.write("ST_INTERSECTS(");
                } else if (filter instanceof Overlaps) {
                    out.write("ST_OVERLAPS(");
                } else if (filter instanceof Touches) {
                    out.write("ST_TOUCHES(");
                } else if (filter instanceof Within) {
                    out.write("ST_WITHIN(");
                } else {
                    throw new RuntimeException("unknown operator: " + filter);
                }

                if (swapped) {
                    e2.accept(this, extraData);
                    out.write(", ");
                    e1.accept(this, extraData);
                } else {
                    e1.accept(this, extraData);
                    out.write(", ");
                    e2.accept(this, extraData);
                }

                out.write(")");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return extraData;
    }
}
