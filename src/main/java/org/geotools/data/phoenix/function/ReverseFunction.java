package org.geotools.data.phoenix.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.StringUtil;

import java.util.List;

/**
 * REVERSE函数
 * Created by Administrator on 2016/12/21.
 */
@FunctionParseNode.BuiltInFunction(name = ReverseFunction.NAME, args = {@FunctionParseNode.Argument(allowedTypes = {PVarchar.class})})
public class ReverseFunction extends ScalarFunction {

    protected final static String NAME = "REVERSE";

    public ReverseFunction() {

    }

    public ReverseFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr))
            return false;
        int targetOffset = ptr.getLength();
        if (targetOffset == 0)
            return true;
        byte[] source = ptr.get();
        byte[] target = new byte[targetOffset];
        int sourceOffset = ptr.getOffset();
        int endOffset = sourceOffset + ptr.getLength();
        SortOrder sortOrder = arg.getSortOrder();
        while (sourceOffset < endOffset) {
            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], sortOrder);
            targetOffset -= nBytes;
            System.arraycopy(source, sourceOffset, target, targetOffset, nBytes);
            sourceOffset += nBytes;
        }
        ptr.set(target);
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}
