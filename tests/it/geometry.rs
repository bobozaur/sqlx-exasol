use sqlx_exasol::types::geo_types::{Geometry, Line, LineString, Point};

use crate::{test_type_array, test_type_valid};

// TODO: Write EMPTY types tests too

test_type_valid!(geometry_point<Geometry>::"GEOMETRY"::("'POINT (1 2)'" => Geometry::Point(Point::new(1.0, 2.0))));
test_type_valid!(geometry_line<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" => Geometry::Line(Line::new((1.0, 2.0), (3.0, 4.0)))));
test_type_valid!(geometry_linestring<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" => Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()]))));
// test_type_valid!(geometry_linearring<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_polygon<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_multilinestring<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_multipoint<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_multipolygon<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_geometry_collection<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_rect<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()])))); test_type_valid!
// (geometry_triangle<Geometry>::"GEOMETRY"::("'LINESTRING (1 2,3 4)'" =>
// Geometry::LineString(LineString(vec![(1.0, 2.0).into(), (3.0, 4.0).into()]))));

test_type_valid!(geometry_option<Option<Geometry>>::"GEOMETRY"::("''" => None::<Geometry>, "NULL" => None::<Geometry>, "'POINT (1 2)'" => Some(Geometry::Point(Point::new(1.0, 2.0)))));

test_type_array!(geometry_array<Geometry>::"GEOMETRY"::(vec![Geometry::Point(Point::new(1.0, 2.0)), Geometry::Point(Point::new(1.0, 2.0))]));
