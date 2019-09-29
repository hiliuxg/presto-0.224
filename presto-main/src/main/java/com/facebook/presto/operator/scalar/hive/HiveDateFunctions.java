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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.LocalDateTime;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class HiveDateFunctions {

    private HiveDateFunctions(){}

    @ScalarFunction(value = "from_unixtime" )
    @Description("Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string " +
            "representing the timestamp of that moment in the current system time zone in the format of \"1970-01-01 00:00:00\".")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixtime(@SqlType(StandardTypes.BIGINT) long unixtime ,
                                     @SqlType(StandardTypes.VARCHAR) Slice format) {
        try{
            Calendar calendar = HiveDateUtil.getCalendar();
            calendar.setTimeInMillis(unixtime * 1000L);
            String result = HiveDateUtil.find(format.toStringUtf8()).format(calendar.getTime()) ;
            return Slices.utf8Slice(result) ;
        }catch (Exception e){
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "fromUnixtime error, argument["+unixtime+" , "+format.toStringUtf8()+"] , message["+e.getMessage()+"]");
        }

    }

    @ScalarFunction(value = "unix_timestamp" ,calledOnNullInput = true)
    @Description("Returns the last day of the month which the date belongs to (as of Hive 1.1.0). " +
            "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. " +
            "The time part of date is ignored.Gets current Unix timestamp in seconds. " +
            "This function is not deterministic and its value is not fixed for the scope of a query execution, " +
            "therefore prevents proper optimization of queries - this has been deprecated since 2.0 in favour of CURRENT_TIMESTAMP constant.")
    @SqlType(StandardTypes.BIGINT)
    public static long unixTimestamp(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice date,
                                     @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice format) {
        try{
            SimpleDateFormat sf = HiveDateUtil.find(format == null ? "yyyy-MM-dd HH:mm:ss" : format.toStringUtf8());
            Date curdate = date == null ? new Date() : sf.parse(date.toStringUtf8()) ;
            return (curdate.getTime() / 1000);
        }catch (Exception e){
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "unix_timestamp error, argument["+date.toStringUtf8()+" , "+format.toStringUtf8()+"] , message["+e.getMessage()+"]");
        }

    }

    @ScalarFunction(value = "unix_timestamp" )
    @Description("Returns the last day of the month which the date belongs to (as of Hive 1.1.0). " +
            "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. " +
            "The time part of date is ignored.Gets current Unix timestamp in seconds. " +
            "This function is not deterministic and its value is not fixed for the scope of a query execution, " +
            "therefore prevents proper optimization of queries - this has been deprecated since 2.0 in favour of CURRENT_TIMESTAMP constant.")
    @SqlType(StandardTypes.BIGINT)
    public static long unixTimestamp(@SqlType(StandardTypes.VARCHAR) Slice date)  {
        return unixTimestamp(date,null);
    }

    @ScalarFunction(value = "unix_timestamp" )
    @Description("Returns the last day of the month which the date belongs to (as of Hive 1.1.0). " +
            "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. " +
            "The time part of date is ignored.Gets current Unix timestamp in seconds. " +
            "This function is not deterministic and its value is not fixed for the scope of a query execution, " +
            "therefore prevents proper optimization of queries - this has been deprecated since 2.0 in favour of CURRENT_TIMESTAMP constant.")
    @SqlType(StandardTypes.BIGINT)
    public static long unixTimestamp() {
        return unixTimestamp(null,null);
    }

    @ScalarFunction("to_date")
    @Description("Returns the date part of a timestamp string to_date(\"1970-01-01 00:00:00\") = \"1970-01-01\"." +
            " As of Hive 2.1.0, returns a date object..")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toDate(@SqlType(StandardTypes.VARCHAR) Slice date ){
        String result = date.toStringUtf8().substring(0,10);
        return Slices.utf8Slice(result) ;
    }

    @ScalarFunction
    @Description("Returns the year part of a date or a timestamp string: year(\"1970-01-01 00:00:00\") = 1970," +
            " year(\"1970-01-01\") = 1970.")
    @SqlType(StandardTypes.INTEGER)
    public static long extract(@SqlType(StandardTypes.VARCHAR) Slice date ,
                               @SqlType(StandardTypes.INTEGER) long start,
                               @SqlType(StandardTypes.INTEGER) long end ) {
        String result = date.toStringUtf8().substring(((Long)start).intValue(),((Long)end).intValue());
        if (result.startsWith("0")){
            result = result.replace("0","") ;
        }
        return Long.valueOf(result) ;
    }
    
    @ScalarFunction("year")
    @Description("Returns the year part of a date or a timestamp string: year(\"1970-01-01 00:00:00\") = 1970," +
            " year(\"1970-01-01\") = 1970.")
    @SqlType(StandardTypes.INTEGER)
    public static long year(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return extract(date,0,4);
    }

    @ScalarFunction("month")
    @Description("Returns the month part of a date or a timestamp string: " +
            "month(\"1970-11-01 00:00:00\") = 11, month(\"1970-11-01\") = 11.")
    @SqlType(StandardTypes.INTEGER)
    public static long month(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return extract(date,5,7);
    }

    @ScalarFunction("day")
    @Description("Returns the month part of a date or a timestamp string: " +
            "month(\"1970-11-01 00:00:00\") = 11, month(\"1970-11-01\") = 11.")
    @SqlType(StandardTypes.INTEGER)
    public static long day(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return extract(date,8,10);
    }

    @ScalarFunction("dayofmonth")
    @Description("Returns the month part of a date or a timestamp string: " +
            "month(\"1970-11-01 00:00:00\") = 11, month(\"1970-11-01\") = 11.")
    @SqlType(StandardTypes.INTEGER)
    public static long dayofmonth(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return day(date) ;
    }

    @ScalarFunction("hour")
    @Description("Returns the hour of the timestamp: hour('2009-07-30 12:58:59') = 12, hour('12:58:59') = 12.")
    @SqlType(StandardTypes.INTEGER)
    public static long hour(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return extract(date,11,13);
    }

    @ScalarFunction("minute")
    @Description("Returns the minute of the timestamp.")
    @SqlType(StandardTypes.INTEGER)
    public static long minute(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return extract(date,14,16);
    }

    @ScalarFunction("second")
    @Description("Returns the second of the timestamp.")
    @SqlType(StandardTypes.INTEGER)
    public static long second(@SqlType(StandardTypes.VARCHAR) Slice date ){
        return extract(date,17,19);
    }

    @ScalarFunction("weekofyear")
    @Description("Returns the week number of a timestamp string: weekofyear(\"1970-11-01 00:00:00\") = 44, " +
            "weekofyear(\"1970-11-01\") = 44.")
    @SqlType(StandardTypes.INTEGER)
    public static long weekofyear(@SqlType(StandardTypes.VARCHAR) Slice date ) {
        try{
            Date tdate = HiveDateUtil.find("yyyy-MM-dd").parse(date.toStringUtf8());
            Calendar calendar = HiveDateUtil.getCalendar();
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
            calendar.setMinimalDaysInFirstWeek(4);
            calendar.setTime(tdate);
            return  calendar.get(Calendar.WEEK_OF_YEAR);
        }catch (Exception e){
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "weekofyear error, argument["+date.toStringUtf8()+"] , message["+e.getMessage()+"]");
        }

    }

    @ScalarFunction("datediff")
    @Description("Returns the number of days from startdate to enddate: datediff('2009-03-01', '2009-02-27') = 2.")
    @SqlType(StandardTypes.INTEGER)
    public static long datediff(@SqlType(StandardTypes.VARCHAR) Slice endDate ,
                                @SqlType(StandardTypes.VARCHAR) Slice startDate )  {
        try{
            SimpleDateFormat sf = HiveDateUtil.find("yyyy-MM-dd");
            Date end = sf.parse(endDate.toStringUtf8());
            Date start = sf.parse(startDate.toStringUtf8());
            long diffInMilliSeconds = end.getTime() - start.getTime();
            return diffInMilliSeconds / (86400 * 1000);
        }catch (Exception e){
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "datediff error, argument["+endDate+","+startDate+"] , message["+e.getMessage()+"]");
        }

    }


    @ScalarFunction("add_months")
    @Description("Returns the date that is num_months after start_date  " +
            "start_date is a string, date or timestamp. num_months is an integer. " +
            "If start_date is the last day of the month or if the resulting month has fewer days than the day component of start_date, " +
            "then the result is the last day of the resulting month. Otherwise, the result has the same day component as start_date. " +
            "The default output format is 'yyyy-MM-dd'.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice addMonths(@SqlType(StandardTypes.VARCHAR) Slice startDate ,
                                  @SqlType(StandardTypes.BIGINT) long numMonths) {
        try{
            SimpleDateFormat dateFormat = HiveDateUtil.find("yyyy-MM-dd");
            Date date = dateFormat.parse(startDate.toStringUtf8());
            Calendar calendar = HiveDateUtil.getCalendar();
            calendar.setTime(date);
            calendar.add(Calendar.MONTH, ((Long)numMonths).intValue());
            return  Slices.utf8Slice(dateFormat.format(calendar.getTime()));
        }catch (Exception e){
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "add_months error, argument["+startDate.toStringUtf8()+","+numMonths+"] , message["+e.getMessage()+"]");
        }
    }

    @ScalarFunction("last_day")
    @Description("Returns the last day of the month which the date belongs to (as of Hive 1.1.0). " +
            "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. The time part of date is ignored.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice lastDay(@SqlType(StandardTypes.VARCHAR) Slice slice ) {
        try{
            Date date = HiveDateUtil.find("yyyy-MM-dd").parse(slice.toStringUtf8());
            Calendar calendar = HiveDateUtil.getCalendar();
            calendar.setTime(date);
            int maxDd = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
            calendar.set(Calendar.DAY_OF_MONTH, maxDd);
            String lastDay = HiveDateUtil.find("yyyy-MM-dd").format(calendar.getTime());
            return  Slices.utf8Slice(lastDay);
        }catch (Exception e){
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "last_day error, argument["+slice.toStringUtf8()+"] , message["+e.getMessage()+"]");
        }

    }

}
