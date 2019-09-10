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

    private static final SimpleDateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");
    private static final SimpleDateFormat MONTH_FORMAT = new SimpleDateFormat("MM");
    private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("dd");
    private static final SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("hh");
    private static final SimpleDateFormat MINUTE_FORMAT = new SimpleDateFormat("mm");
    private static final SimpleDateFormat SECOND_FORMAT = new SimpleDateFormat("ss");

    private static final SimpleDateFormat YEAR_MONTH_FORMAT = new SimpleDateFormat("yyyy-MM");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATE_HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH");
    private static final SimpleDateFormat DATE_MINUTE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static SimpleDateFormat find(String format){
        switch (format){
            case "yyyy" :
                return YEAR_FORMAT ;
            case "MM" :
                return MONTH_FORMAT ;
            case "dd" :
                return DAY_FORMAT ;
            case "hh" :
                return HOUR_FORMAT ;
            case "mm" :
                return MINUTE_FORMAT ;
            case "ss" :
                return SECOND_FORMAT ;
            case "yyyy-MM" :
                return YEAR_MONTH_FORMAT ;
            case "yyyy-MM-dd" :
                return DATE_FORMAT ;
            case "yyyy-MM-dd HH" :
                return DATE_HOUR_FORMAT ;
            case "yyyy-MM-dd HH:mm" :
                return DATE_MINUTE_FORMAT ;
            case "yyyy-MM-dd HH:mm:ss" :
                return TIME_FORMAT ;
                default:
                    return new SimpleDateFormat(format) ;
        }

    }

}
