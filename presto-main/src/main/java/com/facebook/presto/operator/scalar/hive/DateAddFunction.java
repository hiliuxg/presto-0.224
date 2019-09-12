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
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


@ScalarFunction(value = "date_add")
@Description("Adds a number of days to startdate: date_add('2008-12-31', 1) = '2009-01-01'." +
        "the date can be 'yyyy-MM-dd' ")
public final class DateAddFunction {

    private static final SimpleDateFormat DATE_DEFAULT_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final Calendar calendar = Calendar.getInstance();

    private DateAddFunction(){}

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd1(@SqlType(StandardTypes.VARCHAR) Slice slice ,
                                @SqlType("T") long days) throws ParseException {
        Date date = DATE_DEFAULT_FORMAT.parse(slice.toStringUtf8()) ;
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, ((Long)days).intValue());
        String result = DATE_DEFAULT_FORMAT.format(calendar.getTime()) ;
        calendar.clear();
        return Slices.utf8Slice(result) ;
    }

}