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

/**
 * ST_MBRINTERSECTS函数
 * Created by Administrator on 2016/12/28.
 */
@FunctionParseNode.BuiltInFunction(name = MbrintersectsFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PVarchar.class})})
public class MbrintersectsFunction extends ScalarFunction {
    public static final String NAME = "ST_MBRINTERSECTS";

    public MbrintersectsFunction() {}

    public MbrintersectsFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        List<String> params = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
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

        if (geoOne != null && geoTwo != null) {
            if (geoOne.getEnvelope() != null && geoTwo.getEnvelope() != null) {
                getDataType().getCodec().encodeInt(geoOne.getEnvelope().intersects(geoTwo.getEnvelope()) ? 1 : 0, ptr);
            } else {
                throw new NullPointerException("geometry's envelope is null");
            }
        } else {
            throw new IllegalDataException("parse geometry-wkt exception");
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }
}
