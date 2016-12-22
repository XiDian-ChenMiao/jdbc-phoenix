package com.geotools.data.phoenix;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;

/**
 * 关于JTS的关键对象的操作测试类
 * Created by Administrator on 2016/12/22.
 */
public class JtsGeometryOperationTest {

    private final static WKTReader READER = new WKTReader();

    @Test
    public void testDistance() throws ParseException {
        Geometry one = READER.read("POINT (0 0)");
        Geometry two = READER.read("POINT (1 1)");
        System.out.println(one.distance(two));
    }

    public static void main(String[] args) throws ParseException {
        JtsGeometryOperationTest test = new JtsGeometryOperationTest();
        test.testDistance();
    }
}
