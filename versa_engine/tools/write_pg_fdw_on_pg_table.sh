module_dir=$1
work_dir=$2
port=$3
dbhost=$4
dbuser=$5
dbpass=$6
dblocalid=$7
dbdb=$8
dbschema=$9
tbl_name=${10}

export PGPASSWORD="$dbpass"
echo "dbuser = ", $dbuser
echo "database = ", $dbdb
echo $dbhost
echo "dbschema = ", $dbschema
echo $tbl_name

pg_dump -U $dbuser  -d $dbdb -h $dbhost  -s -O -x  -t $dbschema.$tbl_name > ${work_dir}/${tbl_name}.schema_dump
cat ${work_dir}/${tbl_name}.schema_dump | awk 'BEGIN{ offed=0}{if ($0~/CREATE TABLE/){offed=1} else {if (offed){if ($0~/\)\;/){offed=0}else{print $0}}}}' > ${work_dir}/${tbl_name}.schema 
cat ${work_dir}/${tbl_name}.schema_dump | awk 'BEGIN{ offed=0}{if ($0~/CREATE TABLE/){print $0; offed=1} else {if (offed){if ($0~/\)\;/){offed=0;print $0}else{print $0}}}}' > ${work_dir}/${tbl_name}.create_tbl_cmd
awk -f ${module_dir}/convert_pg_to_pg_schema.awk ${work_dir}/${tbl_name}.schema > "$work_dir"/${tbl_name}.wrap_sql.template
#some issue with adc data type which is (MultiPolygon,4326)
sed 's/__TBL__/'$tbl_name'/g' "$work_dir"/${tbl_name}.wrap_sql.template | sed 's/__dblocalid__/'$dblocalid'/'| sed 's/__dbschema__/'${dbschema}'/' | sed 's/,4326//'  | sed 's/public.//'> "$work_dir"/${tbl_name}.wrap_sql

