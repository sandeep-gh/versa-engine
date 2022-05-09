function to_pg_type(type)
{
    if(tolower(type) == "int")
	return "integer"
    if(tolower(type) == "int4")
	return "integer"
    if(tolower(type) == "int8")
	return "integer"    
    
    if(tolower(type) == "integer")
	return "integer"
    if(tolower(type) == "number")
	return "integer"
    if(tolower(type) == "float")
	return "double precision"

    if(tolower(type) == "numeric")
	return "numeric"
    
    if(tolower(type) == "varchar")
	return "varchar"

    if(tolower(type) == "varchar(254)")
	return "varchar"
    
    if(tolower(type) == "string")
	return "varchar"

    if(tolower(type) == "date")
        return "varchar"

    if(tolower(type) == "percent")
        return "varchar"

    if(tolower(type) == "interval")
        return "IntRangeType"
    if(tolower(type) == "raster")
        return "Raster"
    
    if(tolower(type) == "polygon")
        return "Geometry('POLYGON')"

    if(tolower(type) == "pointzm")
        return "Geometry('POINTZM')"
    
    if(tolower(type) == "polygon_2d")
        return "Geometry('POLYGON')"
    if(tolower(type) == "line_2d")
        return "Geometry('LINE')"
    if(tolower(type) == "point_2d")
        return "Geometry('POINT')"

    if(tolower(type) == "multipolygon")
        return "Geometry('MULTIPOLYGON')"
    if(tolower(type) == "multipolygon_2d")
        return "Geometry('MULTIPOLYGON')"
    if(tolower(type) == "multiline_2d")
        return "Geometry('MULTILINE')"
    if(tolower(type) == "multipoint_2d")
        return "Geometry('MULTIPOINT')"
    print "9unkown type" type
}

BEGIN{
 str="create table __SCHEMA__.__TBL__ ("
}
{
    str = str $1 " " to_pg_type($2) " , "
}
END{
    str = substr(str, 0, length(str) -2)
    str= str  ")"
    print str
}
