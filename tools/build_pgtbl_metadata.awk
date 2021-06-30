function get_type(schema_line)
{
    gsub(/NOT/, "", schema_line);
    gsub(/NULL/, "", schema_line);
    gsub(/,/, "", schema_line)
    return schema_line
    
}


BEGIN{
 str="<metadata>\n<model>__TBL__</model>\n<columns>\n"
}
{
    type=get_type($2)
    for(i = 3; i<=NF; ++i)
    {
	type=type" "get_type($i)
    }
    gsub(/[ \t]+$/,"",type)
    str = str "<column><name>"$1"</name><type>"type"</type></column>\n"
}

END{
    str= str  "</columns>\n<primarykey></primarykey>\n</metadata>"
    print str
}
