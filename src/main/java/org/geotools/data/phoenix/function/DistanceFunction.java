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
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * ST_DISTANCE_EXT函数
 * Created by Administrator on 2016/12/22.
 */
@FunctionParseNode.BuiltInFunction(name = DistanceFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PVarchar.class, PVarchar.class, PVarchar.class})})
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
        List<String> params = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Expression param = children.get(i);
            if (!param.evaluate(tuple, ptr))
                return false;
            params.add((String) PVarchar.INSTANCE.toObject(ptr, param.getDataType()));
        }

        Geometry geoOne, geoTwo;
        try {
            geoOne = new WKTReader().read(params.get(0));
            geoTwo = new WKTReader().read(params.get(1));
        } catch (ParseException e) {
            throw new IllegalDataException("parse geometry-wkt exception");
        }

        double compareValue;
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        if (pattern.matcher(params.get(3)).matches()) {
            compareValue = Double.parseDouble(params.get(3));
        } else {
            throw new IllegalDataException("parse double parameter exception");
        }
        if (geoOne != null && geoTwo != null) {
            Double distance = geoOne.distance(geoTwo);/*两空间对象间的距离*/
            ptr.set(new byte[getDataType().getByteSize()]);
            if (">".equals(params.get(2)))
                getDataType().getCodec().encodeInt(distance > compareValue ? 1 : 0, ptr);
            else if ("<".equals(params.get(2)))
                getDataType().getCodec().encodeInt(distance < compareValue ? 1 : 0, ptr);
            else
                throw new IllegalDataException("parse comparator exception");
        } else {
            throw new IllegalDataException("parse geometry-wkt exception");
        }
        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }
}
