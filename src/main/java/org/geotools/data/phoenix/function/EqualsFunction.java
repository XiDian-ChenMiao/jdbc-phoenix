package org.geotools.data.phoenix.function;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.ArrayList;
import java.util.List;

/**
 * ST_EQUALS函数
 * Created by Administrator on 2016/12/22.
 */
public class EqualsFunction extends ScalarFunction {

    protected final static String NAME = "ST_EQUALS";

    public EqualsFunction() {

    }

    public EqualsFunction(List<Expression> children) {
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
            getDataType().getCodec().encodeInt(geoOne.equals(geoTwo) ? 1 : 0, ptr);
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

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }
}
