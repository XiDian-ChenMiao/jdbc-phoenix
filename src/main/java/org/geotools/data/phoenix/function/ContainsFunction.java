package org.geotools.data.phoenix.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * ST_CONTAINS函数
 * Created by Administrator on 2016/12/22.
 */
@FunctionParseNode.BuiltInFunction(name = ContainsFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PVarchar.class})})
public class ContainsFunction extends ScalarFunction {

    protected final static String NAME = "ST_CONTAINS";

    @Override
    public String getName() {
         return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return false;
    }

    @Override
    public PDataType getDataType() {
        return null;
    }
}
