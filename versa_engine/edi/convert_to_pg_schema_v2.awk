function to_pg_type(type)
{
    if(tolower(type) == "int")
        return "integer"
    if(tolower(type) == "integer")
	return "integer"
    if(tolower(type) == "number")
	return "numeric"
    if(tolower(type) == "float")
	return "double precision"
    if(tolower(type) == "varchar")
	return "varchar"
    if(tolower(type) == "string")
	return "varchar"
    if(tolower(type) == "polygon_2d")
        return "Geometry('POLYGON')"
    if(tolower(type) == "line_2d")
        return "Geometry('LINE')"
    if(tolower(type) == "point_2d")
        return "Geometry('POINT')"
    if(tolower(type) == "multipolygon_2d")
        return "Geometry('MULTIPOLYGON')"
    if(tolower(type) == "multiline_2d")
        return "Geometry('MULTILINE')"
    if(tolower(type) == "multipoint_2d")
        return "Geometry('MULTIPOINT')"
    if(tolower(type) == "interval")
        return "IntRangeType"
    if(tolower(type) == "raster")
        return "Raster"
    print "7unkown type " type
}


BEGIN{
 str="create foreign table __TBL___fdw ("
}
{
    str = str $1 " " to_pg_type($2) " , "
}

END{
    str = substr(str, 0, length(str) -2)
    str= str  ")  server __dblocalid___server OPTIONS (schema '__dbschema__' ,table '__TBL__' )"
    print str
}
