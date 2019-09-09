package com.facebook.presto.operator.scalar.hive;

import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;

@ScalarFunction(value = "isnotnull", calledOnNullInput = true)
@Description("Returns TRUE if the argument is not NULL")
public final class IsNotNullFunction
{
    private IsNotNullFunction(){}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNotNull(@SqlNullable @SqlType("T") Object value)
    {
        return value != null;
    }

}
