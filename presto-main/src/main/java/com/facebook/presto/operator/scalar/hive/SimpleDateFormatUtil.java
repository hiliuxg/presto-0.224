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

import java.text.SimpleDateFormat;

public class SimpleDateFormatUtil {

    private static final ThreadLocal<SimpleDateFormat> YEAR_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy"));
    private static final ThreadLocal<SimpleDateFormat> MONTH_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("MM"));
    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("dd"));
    private static final ThreadLocal<SimpleDateFormat> HOUR_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("hh"));
    private static final ThreadLocal<SimpleDateFormat> MINUTE_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("mm"));
    private static final ThreadLocal<SimpleDateFormat> SECOND_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("ss"));

    private static final ThreadLocal<SimpleDateFormat> YEAR_MONTH_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM"));
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
    private static final ThreadLocal<SimpleDateFormat> DATE_HOUR_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH"));
    private static final ThreadLocal<SimpleDateFormat> DATE_MINUTE_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm"));
    private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

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
                    return new SimpleDateFormat(format) ;
        }

    }

}
