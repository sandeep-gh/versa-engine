function to_pg_type(type)
{
    if(tolower(type) == "integer")
	return "integer"
    if(tolower(type) == "number")
	return "integer"
    if(tolower(type) == "float")
	return "double precision"
    if(tolower(type) == "varchar")
	return "varchar"
    if(tolower(type) == "string")
	return "varchar"
    if(tolower(type) == "polygon")
        return "Geometry('POLYGON')"
    if(tolower(type) == "pointzm")
        return "Geometry('PointZM')"
    if(tolower(type) == "interval")
        return "IntRangeType"
    if(tolower(type) == "raster")
        return "Raster"
    print "unkown type " type
}

BEGIN{
 str="create view __VIEW__ ("
}
{
    str = str $1  " , "
}
END{
    str = substr(str, 0, length(str) -2)
    str= str  ") as select * from __TBL__"
    print str
}
