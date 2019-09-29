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
import com.facebook.presto.spi.StandardErrorCode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class HiveDateUtil {

    private static final ThreadLocal<SimpleDateFormat> YEAR_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> MONTH_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("MM");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("dd");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> HOUR_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("hh");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> MINUTE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("mm");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> SECOND_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("ss");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> YEAR_MONTH_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> DATE_HOUR_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> DATE_MINUTE_FORMAT =  ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sf;
    });

    private static final ThreadLocal<Calendar> CALENDAR = ThreadLocal.withInitial(() -> {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return calendar ;
    });

    public static Calendar getCalendar(){
        Calendar calendar = CALENDAR.get();
        calendar.clear();
        return calendar ;
    }

    public static SimpleDateFormat find(String format){
        switch (format){
            case "yyyy" :
                return YEAR_FORMAT.get() ;
            case "MM" :
                return MONTH_FORMAT.get() ;
            case "dd" :
                return DAY_FORMAT.get() ;
            case "hh" :
                return HOUR_FORMAT.get() ;
            case "mm" :
                return MINUTE_FORMAT.get() ;
            case "ss" :
                return SECOND_FORMAT.get() ;
            case "yyyy-MM" :
                return YEAR_MONTH_FORMAT.get() ;
            case "yyyy-MM-dd" :
                return DATE_FORMAT.get() ;
            case "yyyy-MM-dd HH" :
                return DATE_HOUR_FORMAT.get() ;
            case "yyyy-MM-dd HH:mm" :
                return DATE_MINUTE_FORMAT.get() ;
            case "yyyy-MM-dd HH:mm:ss" :
                return TIME_FORMAT.get() ;
                default:
                    throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT,"Not support format " + format);
        }

    }

}
