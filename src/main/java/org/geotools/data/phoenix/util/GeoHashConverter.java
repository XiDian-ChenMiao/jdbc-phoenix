package org.geotools.data.phoenix.util;

import java.util.HashMap;

/**
 * 经纬度与GEOHASH的互转工具类
 * Created by hadoop on 2016/5/20.
 */
public class GeoHashConverter {
    public static final int numBits = 6 * 5;
    public static final int MAX_HASH_LENGTH = 12;

    final static char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
            '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p',
            'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    final static HashMap<Character, Integer> lookup = new HashMap<Character, Integer>();

    static {
        int i = 0;
        for (char c : digits)
            lookup.put(c, i++);
    }

    /**
     * 私有化构造函数，形成工具类
     */
    private GeoHashConverter() {

    }

    /**
     * 函数功能：double类型的经纬度坐标转long类型的GeoHash，经测试，转码正确，但long的编码二进制是64位
     *
     * @param longitude 经度
     * @param latitude  纬度
     * @return long类型的GeoHash
     */
    public static long longAndLatiToGeohash(double longitude, double latitude) {
        boolean isEven = true;
        double minLat = -90.0, maxLat = 90.0;
        double minLon = -180.0, maxLon = 180.0;
        long bit = 0x8000000000000000L;
        long g = 0x0L;

        long target = 0x8000000000000000L >>> (5 * MAX_HASH_LENGTH);
        while (bit != target) {
            if (isEven) {
                double mid = (minLon + maxLon) / 2;
                if (longitude >= mid) {
                    g |= bit;
                    minLon = mid;
                } else
                    maxLon = mid;
            } else {
                double mid = (minLat + maxLat) / 2;
                if (latitude >= mid) {
                    g |= bit;
                    minLat = mid;
                } else
                    maxLat = mid;
            }

            isEven = !isEven;
            bit >>>= 1;
        }
        return g;
    }

    /**
     * 函数功能：long类型的GeoHash值转double类型的经纬度坐标
     *
     * @param geoHash long类型的geoHash值
     * @return doule类型的经纬度坐标
     */
    public static double[] geohashToLongAndLati(Long geoHash) {
        boolean isEven = true;
        double[] lat = new double[2];
        double[] lon = new double[2];
        lat[0] = -90.0;
        lat[1] = 90.0;
        lon[0] = -180.0;
        lon[1] = 180.0;
        long bit = 0x8000000000000000L;
        for (int i = 0; i < 5 * MAX_HASH_LENGTH; i++) {
            boolean bitValue = (geoHash & bit) != 0;
            if (isEven) {
                refineInterval(lon, bitValue);
            } else {
                refineInterval(lat, bitValue);
            }
            isEven = !isEven;
            bit >>>= 1;
        }
        double resultLat = (lat[0] + lat[1]) / 2;
        double resultLon = (lon[0] + lon[1]) / 2;
        return new double[]{resultLon, resultLat};
    }

    /**
     * Refines interval by a factor or 2 in either the 0 or 1 ordinate.
     *
     * @param interval two entry array of double values
     * @param bitValue
     */
    private static void refineInterval(double[] interval, boolean bitValue) {
        if (bitValue)
            interval[0] = (interval[0] + interval[1]) / 2;
        else
            interval[1] = (interval[0] + interval[1]) / 2;
    }
}
