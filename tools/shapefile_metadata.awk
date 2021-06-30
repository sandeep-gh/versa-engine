BEGIN{
    FS=","
    schema_lines = 0
}
{
    if (schema_lines == 0) {
        if ($0 ~/CREATE TABLE/)
        {
            schema_lines = 1
            next
        }
    }
    if ($0 ~/ALTER/){
        schema_lines = 0
    }
    if ($0 ~/AddGeometryColumn/){
        print("geom", substr($5, 2, length($5)-2))
    }

    if (schema_lines == 1){
        l = $0
        sub(",$", "", l); sub(");$", "", l); sub("^\"", "", l); sub("\" ", " ", l)
        print l


    }

}
END{
}
