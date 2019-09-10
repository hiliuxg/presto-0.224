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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Base64;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HiveStringFunctions {

    private  static MessageDigest md5;

    static {
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {}
    }

    private HiveStringFunctions() { }

    @ScalarFunction("ascii")
    @Description("Returns the numeric value of the first character of str.")
    @SqlType(StandardTypes.INTEGER)
    public static long ascii(@SqlType(StandardTypes.VARCHAR) Slice slice )
    {
        String s = slice.toStringUtf8() ;
        if (s.length() > 0) {
            return s.getBytes()[0];
        } else {
            return 0 ;
        }
    }

    @ScalarFunction("md5")
    @Description("md5")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice md5(@SqlType(StandardTypes.VARCHAR) Slice slice) throws UnsupportedEncodingException {
        md5.update(slice.toStringUtf8().getBytes());
        byte s[] = md5.digest();
        String result = "";
        for (int i = 0; i < s.length; i++) {
            result += Integer.toHexString((0x000000FF & s[i]) | 0xFFFFFF00).substring(6);
        }
        return Slices.utf8Slice(result);
    }

    @ScalarFunction("md532")
    @Description("md5")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice md532(@SqlType(StandardTypes.VARCHAR) Slice slice) throws UnsupportedEncodingException {
        return md5(slice) ;
    }

    @ScalarFunction("encodeBase64")
    @Description("encodeBase64")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice encodeBase64(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        byte[] bytes = slice.toStringUtf8().getBytes();
        return Slices.utf8Slice(new String(Base64.getEncoder().encode(bytes)));
    }

    @ScalarFunction("decodeBase64")
    @Description("decodeBase64")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice decodeBase64(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        byte[] bytes  = slice.toStringUtf8().getBytes();
        return Slices.utf8Slice(new String(Base64.getDecoder().decode(bytes)));
    }


}
