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
	return ntype
    }
    if(tolower(type)~/char\([0-9]+\)/)
    {
	return varchar
    }
    if(tolower(type)~/number\([0-9]+\)/)
    {
	return "numeric"
    }

    print "unkown type " type
}


BEGIN{
 str="<metadata>\n<model>__MDN__</model>\n<columns>\n"
}
{
    str = str "<column><name>"$1"</name><type>"to_pg_type($2)"</type></column>\n"
}

END{
    str= str  "</columns>\n<primarykey></primarykey>\n</metadata>"
    print str
}
