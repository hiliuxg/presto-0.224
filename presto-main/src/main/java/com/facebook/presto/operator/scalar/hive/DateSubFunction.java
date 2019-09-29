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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Calendar;
import java.util.Date;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;


@ScalarFunction(value = "date_sub")
@Description("Subtracts a number of days to startdate: date_sub('2008-12-31', 1) = '2008-12-30'.." +
        "the date can be 'yyyy-MM-dd' ")
public final class DateSubFunction {

    private DateSubFunction(){}

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSub(@SqlType(StandardTypes.VARCHAR) Slice slice ,
                                @SqlType("T") long days) {
        try{
            Calendar calendar = Calendar.getInstance();
            Date date = HiveDateUtil.find("yyyy-MM-dd").parse(slice.toStringUtf8()) ;
            calendar.setTime(date);
            calendar.add(Calendar.DAY_OF_MONTH, -((Long)days).intValue());
            String result = HiveDateUtil.find("yyyy-MM-dd").format(calendar.getTime()) ;
            return Slices.utf8Slice(result) ;
        }catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "date_sub error, argument["+slice.toStringUtf8()+" , "+days+"] , message["+e.getMessage()+"]");
        }
    }

}
