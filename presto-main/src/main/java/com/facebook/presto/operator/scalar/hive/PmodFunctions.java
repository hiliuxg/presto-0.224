/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar.hive;


import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

public class PmodFunctions {

    private PmodFunctions() { }

    @ScalarFunction("pmod")
    @Description("Returns the positive value of a mod b.ss")
    @SqlType(StandardTypes.TINYINT)
    public static long pmod(@SqlType(StandardTypes.TINYINT) long  left , @SqlType(StandardTypes.TINYINT) long  right)
    {
        return ((left % right) + right) % right ;
    }

    @ScalarFunction("pmod")
    @Description("Returns the positive value of a mod b.ss")
    @SqlType(StandardTypes.SMALLINT)
    public static long pmod1(@SqlType(StandardTypes.SMALLINT) long  left , @SqlType(StandardTypes.SMALLINT) long  right)
    {
        return pmod(left,right) ;
    }


    @ScalarFunction("pmod")
    @Description("Returns the positive value of a mod b.ss")
    @SqlType(StandardTypes.INTEGER)
    public static long pmod2(@SqlType(StandardTypes.INTEGER) long  left , @SqlType(StandardTypes.INTEGER) long  right)
    {
        return pmod(left,right) ;
    }

    @ScalarFunction("pmod")
    @Description("Returns the positive value of a mod b.ss")
    @SqlType(StandardTypes.BIGINT)
    public static long pmod3(@SqlType(StandardTypes.BIGINT) long  left , @SqlType(StandardTypes.BIGINT) long  right)
    {
        return pmod(left,right) ;
    }

    @ScalarFunction("pmod")
    @Description("Returns the positive value of a mod b.ss")
    @SqlType(StandardTypes.DOUBLE)
    public static double pmodFromFloating(@SqlType(StandardTypes.DOUBLE) double  left , @SqlType(StandardTypes.DOUBLE) double right)
    {
        return ((left % right) + right) % right ;
    }


}
