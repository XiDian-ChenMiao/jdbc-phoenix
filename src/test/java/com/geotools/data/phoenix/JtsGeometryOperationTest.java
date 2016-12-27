package com.geotools.data.phoenix;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.Test;

/**
 * 关于JTS的关键对象的操作测试类
 * NOTE:
 * 相等(Equals)：几何形状拓扑上相等。
 * 脱节(Disjoint)：几何形状没有共有的点。
 * 相交(Intersects)：几何形状至少有一个共有点（区别于脱节）
 * 接触(Touches)：几何形状有至少一个公共的边界点，但是没有内部点。
 * 交叉(Crosses)：几何形状共享一些但不是所有的内部点。
 * 内含(Within)：几何形状A的线都在几何形状B内部。
 * 包含(Contains)：几何形状B的线都在几何形状A内部（区别于内含）
 * 重叠(Overlaps)：几何形状共享一部分但不是所有的公共点，而且相交处有他们自己相同的区域。
 */
public class JtsGeometryOperationTest {

    private final static WKTReader READER = new WKTReader(JTSFactoryFinder.getGeometryFactory());

    @Test
    public void testDistance() throws ParseException {
        Geometry one = READER.read("POINT (0 0)");
        Geometry two = READER.read("POINT (1 1)");
        Point point = (Point) two;
        System.out.println("longtitude:" + point.getCoordinate().getOrdinate(0));
        System.out.println("lantitude:" + point.getCoordinate().getOrdinate(1));
        System.out.println("distance:" + one.distance(two));
    }

    @Test
    public void testEquals() throws ParseException {
        Geometry one = READER.read("POINT (0 0)");
        Geometry two = READER.read("POINT (0 0)");
        System.out.println("equals:" + one.equals(two));
    }

    @Test
    public void testContains() throws ParseException {
        Geometry one = READER.read("POLYGON ((0 0, 2 0, 2 5, 0 5, 0 0))");
        Geometry two = READER.read("POINT (1 1)");
        System.out.println("contains:" + one.contains(two));
    }

    @Test
    public void testCrosses() throws ParseException {
        Geometry one = READER.read("LINESTRING (0 2, 2 0)");
        Geometry two = READER.read("LINESTRING (0 0, 2 2)");
        System.out.println("crosses:" + one.crosses(two));
    }

    /**
     * 几何对象没有交点(相邻)
     * @throws ParseException
     */
    @Test
    public void testDisjoint() throws ParseException {
        Geometry one = READER.read("LINESTRING (0 2, 2 0)");
        Geometry two = READER.read("LINESTRING (0 0, 2 2)");
        System.out.println("disjoint:" + one.disjoint(two));
    }

    @Test
    public void testWithin() throws ParseException {
        Geometry one = READER.read("POLYGON ((0 0, 2 0, 2 5, 0 5, 0 0))");
        Geometry two = READER.read("POINT (1 1)");
        System.out.println("within:" + two.within(one));
    }

    /**
     * 至少一个公共点(相交)
     * @throws ParseException
     */
    @Test
    public void testIntersect() throws ParseException {
        WKTReader reader = new WKTReader(JTSFactoryFinder.getGeometryFactory());
        LineString one = (LineString) reader.read("LINESTRING(0 0, 2 0, 5 0)");
        LineString two = (LineString) reader.read("LINESTRING(0 1, 0 2)");
        Geometry interPoint = one.intersection(two);
        System.out.println("intersects geometry:" + interPoint.toText());
        System.out.println("intersects:" + one.intersects(two));
        System.out.println("disjoint:" + one.disjoint(two));
    }

    public static void main(String[] args) throws ParseException {
        JtsGeometryOperationTest test = new JtsGeometryOperationTest();
        test.testDistance();
        test.testEquals();
        test.testContains();
        test.testCrosses();
        test.testDisjoint();
        test.testWithin();
        test.testIntersect();
    }
}
