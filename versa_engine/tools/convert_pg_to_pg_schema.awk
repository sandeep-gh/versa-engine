function to_pg_type(type)
{
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
    if(tolower(type)~/varchar2\([0-9]+\)/)
    {
	ntype=tolower(type)
	sub("varchar2", "varchar", ntype)
	#return ntype
	return "varchar"
    }
    if(tolower(type) == "date")
	return "date"
    if(tolower(type) == "nclob")
	return "text"
    if(tolower(type)~/nvarchar\([0-9]+\)/)
    {
	ntype=tolower(type)
	sub("nvarchar", "varchar", ntype)
	#return ntype
	return "varchar"
    }
    if(tolower(type)~/nvarchar2\([0-9]+\)/)
    {
	ntype=tolower(type)
	sub("nvarchar2", "varchar", ntype)
	#return ntype
	return "varchar"
    }
    if(tolower(type)~/number\([0-9]+\)/)
    {
	return "numeric"
    }

    print "unkown type " type
}


BEGIN{
 str="create foreign table __TBL___fdw ("
}
{
    str = str $0
}

END{
    
    str= str  ")  server __dblocalid___server OPTIONS (schema_name '__dbschema__' ,table_name '__TBL__' )"
    print str
}
