use sqlx::types::geo_types::{GeometryCollection, Rect, Triangle};
use sqlx_exasol::types::geo_types::{
    Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};

use crate::{test_type_array, test_type_valid};

test_type_valid!(geometry_point<Geometry>::"GEOMETRY"::(
    "'POINT (1 2)'" => Geometry::Point(Point::new(1.0, 2.0)),
    // This is how [`geo-types`] represents an empty [`Point`].
    "'POINT EMPTY'" => Geometry::MultiPoint(MultiPoint::<f64>::empty())
));
test_type_valid!(geometry_linestring<Geometry>::"GEOMETRY"::(
    "'LINESTRING (1 2, 3 4)'" =>
    Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])),
    "'LINESTRING EMPTY'" =>
    Geometry::LineString(LineString::<f64>::empty()),
    "'LINEARRING (1 2, 3 4, 5 6, 1 2)'" =>
    Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into(), (5.0, 6.0).into(), (1.0, 2.0).into()])),
    "'LINEARRING EMPTY'" =>
    Geometry::LineString(LineString::<f64>::empty())
));
test_type_valid!(geometry_polygon<Geometry>::"GEOMETRY"::(
"'POLYGON((1 2, 3 4, 5 6, 1 2))'" =>
Geometry::Polygon(Polygon::new(
    LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into(), (5.0, 6.0).into()]),
    Vec::new())
),
"'POLYGON((5 1, 5 5, 10 5, 5 1), (6 2, 6 3, 7 3, 7 2, 6 2))'" =>
Geometry::Polygon(Polygon::new(
    LineString(vec![(5.0, 1.0).into(), (5.0, 5.0).into(), (10.0, 5.0).into()]),
    vec![LineString(vec![(6.0, 2.0).into(), (6.0, 3.0).into(), (7.0, 3.0).into(), (7.0, 2.0).into()])])),
"'POLYGON EMPTY'" =>
Geometry::Polygon(Polygon::<f64>::empty())
));

test_type_valid!(geometry_multilinestring<Geometry>::"GEOMETRY"::(
"'MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))'" =>
Geometry::MultiLineString(MultiLineString::new(vec![
    LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()]),
    LineString(vec![(5.0, 6.0).into(), (7.0, 8.0).into()])])),
"'MULTILINESTRING EMPTY'" =>
Geometry::MultiLineString(MultiLineString::<f64>::empty())
));
test_type_valid!(geometry_multipoint<Geometry>::"GEOMETRY"::(
    "'MULTIPOINT (1 2, 3 4)'" =>
    Geometry::MultiPoint(MultiPoint::new(vec![Point::new(1.0, 2.0), Point::new(3.0, 4.0)])),
    "'MULTIPOINT EMPTY'" =>
    Geometry::MultiPoint(MultiPoint::<f64>::empty())
));
test_type_valid!(geometry_multipolygon<Geometry>::"GEOMETRY"::(
"'MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)), ((5 1, 5 5, 10 5, 5 1), (6 2, 6 3, 7 3, 7 2, 6 2)))'" =>
Geometry::MultiPolygon(MultiPolygon::new(vec![
    Polygon::new(
        LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into(), (5.0, 6.0).into()]),
        Vec::new()
    ),
    Polygon::new(
        LineString(vec![(5.0, 1.0).into(), (5.0, 5.0).into(), (10.0, 5.0).into()]),
        vec![LineString(vec![(6.0, 2.0).into(), (6.0, 3.0).into(), (7.0, 3.0).into(), (7.0, 2.0).into()])]
    )])),
"'MULTIPOLYGON EMPTY'" => 
Geometry::MultiPolygon(MultiPolygon::<f64>::empty())
));
test_type_valid!(geometry_geometry_collection<Geometry>::"GEOMETRY"::(
"'GEOMETRYCOLLECTION (POINT(1 2), LINESTRING(1 2, 3 4))'" =>
Geometry::GeometryCollection(GeometryCollection(vec![
    Geometry::Point(Point::new(1.0, 2.0)),
    Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()]))])),
"'GEOMETRYCOLLECTION EMPTY'" => 
Geometry::GeometryCollection(GeometryCollection::<f64>::empty())
));

test_type_valid!(geometry_linestring_special<Geometry>::"GEOMETRY"::(
    "'LINESTRING (1 2,3 4)'" =>
    Geometry::Line(Line::new((1.0, 2.0), (3.0, 4.0))) =>
    Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()]))
));
test_type_valid!(geometry_polygon_special<Geometry>::"GEOMETRY"::(
"'POLYGON ((5 1, 5 5, 10 5, 10 1, 5 1))'" =>
Geometry::Rect(Rect::new((5.0, 5.0), (10.0, 1.0))) =>
Geometry::Polygon(Polygon::new(
    LineString(vec![
        (5.0, 1.0).into(),
        (5.0, 5.0).into(),
        (10.0, 5.0).into(),
        (10.0, 1.0).into(),
    ]),
    Vec::new()))
));

// Defined in Counter-ClockWise order (CCW).
test_type_valid!(geometry_triangle<Geometry>::"GEOMETRY"::(
"'POLYGON ((10 1, 10 5, 5 1, 10 1))'" =>
Geometry::Triangle(Triangle::new(
    (10.0, 1.0).into(),
    (10.0, 5.0).into(),
    (5.0, 1.0).into(),
)) =>
Geometry::Polygon(Polygon::new(
    LineString(vec![
        (10.0, 1.0).into(),
        (10.0, 5.0).into(),
        (5.0, 1.0).into(),
    ]),
    Vec::new()))
));

test_type_valid!(geometry_option<Option<Geometry>>::"GEOMETRY"::("''" => None::<Geometry>, "NULL" => None::<Geometry>, "'POINT (1 2)'" => Some(Geometry::Point(Point::new(1.0, 2.0)))));

test_type_array!(geometry_array<Geometry>::"GEOMETRY"::(vec![Geometry::Point(Point::new(1.0, 2.0)), Geometry::Point(Point::new(1.0, 2.0))]));
