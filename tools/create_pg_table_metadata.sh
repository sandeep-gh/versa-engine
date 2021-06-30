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
pg_dump -U $dbuser  -d $dbdb -h $dbhost  -s -O -x  -t $dbschema.$tbl_name > ${work_dir}/${tbl_name}.schema_dump
cat ${work_dir}/${tbl_name}.schema_dump | awk 'BEGIN{ offed=0}{if ($0~/CREATE TABLE/){offed=1} else {if (offed){if ($0~/\)\;/){offed=0}else{print $0}}}}' > ${work_dir}/${tbl_name}.schema 






