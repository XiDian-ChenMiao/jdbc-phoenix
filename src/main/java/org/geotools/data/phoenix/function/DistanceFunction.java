package org.geotools.data.phoenix.function;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.List;

/**
 * ST_DISTANCE函数
 * @param PVarchar 第一个空间对象参数构成的WKT
 * @param PVarchar 第二个空间对象参数构成的WKT
 * Created by Administrator on 2016/12/22.
 */
@FunctionParseNode.BuiltInFunction(name = DistanceFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PVarchar.class})})
public class DistanceFunction extends ScalarFunction {

    protected final static String NAME = "ST_DISTANCE";

    public DistanceFunction() {}

    public DistanceFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression oneParam = children.get(0);
        if (!oneParam.evaluate(tuple, ptr))
            return false;
        Geometry geoOne = null;
        try {
            geoOne = new WKTReader().read((String) PVarchar.INSTANCE.toObject(ptr, oneParam.getDataType()));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Expression twoParam = children.get(1);
        if (!oneParam.evaluate(tuple, ptr))
            return false;
        Geometry geoTwo = null;
        try {
            geoTwo = new WKTReader().read((String) PVarchar.INSTANCE.toObject(ptr, twoParam.getDataType()));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (geoOne != null && geoTwo != null) {
            Double distance = geoOne.distance(geoTwo);/*两空间对象间的距离*/
            ptr.set(PDouble.INSTANCE.toBytes(distance));/*返回距离结果*/
        } else {
            throw new IllegalDataException("parse geometry-wkt exception");
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDouble.INSTANCE;
    }
}
