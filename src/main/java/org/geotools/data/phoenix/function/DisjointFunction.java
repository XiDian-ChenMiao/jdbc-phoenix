package org.geotools.data.phoenix.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * ST_DISJOINT函数
 * Created by Administrator on 2016/12/22.
 */
@FunctionParseNode.BuiltInFunction(name = DisjointFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PVarchar.class})})
public class DisjointFunction extends ScalarFunction {

    protected final static String NAME = "ST_DISJOINT";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable immutableBytesWritable) {
        return false;
    }

    @Override
    public PDataType getDataType() {
        return null;
    }
}
