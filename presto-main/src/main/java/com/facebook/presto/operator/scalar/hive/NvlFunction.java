package com.facebook.presto.operator.scalar.hive;

import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;

@ScalarFunction(value = "nvl", calledOnNullInput = true)
@Description("Returns TRUE if the argument is NULL")
public final class NvlFunction
{
    private NvlFunction(){}

    @TypeParameter("T")
    @SqlType("T")
    public static Object isNull(@SqlNullable @SqlType("T") Object value ,@SqlType("T") Object defaultVal)
    {
        if (value == null){
            value = defaultVal ;
        }
        return value;
    }

}
