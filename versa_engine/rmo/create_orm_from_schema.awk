function to_python_type(type)
{
    if(tolower(type) == "int")
	return "Integer"
    if(tolower(type) == "integer")
	return "Integer"
    if(tolower(type) == "int4")
        return "Integer"
    if(tolower(type) == "int8")
        return "BigInteger"
    if(tolower(type) == "number")
	return "Integer"
    if(tolower(type) == "float")
	return "Float"
    if(tolower(type) == "varchar")
	return "String"
    if(tolower(type) == "string")
	return "String"
    if(tolower(type) == "text")
	return "Text"
    if(tolower(type) == "date")
	return "Date"
    if(tolower(type) == "polygon")
        return "Geometry('MULTIPOLYGON')"
    if(tolower(type) == "multipolygon")
        return "Geometry('MULTIPOLYGON')"
    if(tolower(type) == "pointzm")
        return "Geometry('PointZM')"
    if(tolower(type) == "interval")
        return "IntRangeType"
    if(tolower(type) == "raster")
        return "Raster"
    #oracle data types
    if(tolower(type) == "numeric")
	return "Integer"
    if(tolower(type)~/varchar/)
    {   
        return "String"
    }
    print "unkown type " type
}

BEGIN{
 str="__CLASS_DECORATOR__\nclass __MODEL__ (Base):\n\t __tablename__='__TBL__'\n"
}
{
    str  = str "\t "tolower($1)  "= Column("to_python_type($2)")\n"
}

END{
    str= str  "\t __table_args__ = (\n \
	  PrimaryKeyConstraint(__PKEY_STRING__),\n \
 	  {},\n \
	  )\n\t _foreignkeys=__FOREIGN_KEYS_STRING__"

    print str
}
