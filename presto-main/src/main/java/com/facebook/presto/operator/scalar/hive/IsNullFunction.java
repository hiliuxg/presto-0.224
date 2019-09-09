package com.facebook.presto.operator.scalar.hive;

import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;

@ScalarFunction(value = "isnull", calledOnNullInput = true)
@Description("Returns TRUE if the argument is NULL")
public final class IsNullFunction
{
    private IsNullFunction(){}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNull(@SqlNullable @SqlType("T") Object value)
    {
        return value == null;
    }

}
