EXPECTED_ARGS=9
if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` {arg}"
  exit $E_BADARGS
fi
if [[ `hostname -s` = sfx* ]]; then
    . /etc/profile.d/modules.sh
    module add db/oracle/client/11.2.0
    module load ndssl/postgresql/9.3beta2
else 
    #TODO: figure out if we are on dedicated_q
    . /etc/profile.d/modules.sh
    module add db/oracle/client/11.2.0
    module load ndssl/postgresql/9.3beta2
fi

module_dir=$1
work_dir=$2
port=$3
dblocalid=$4
dbuser=$5
dbpass=$6
dbschema=$7
tbl_name=$8
model_name=$9

sed 's/_TBL_/'$tbl_name'/' "$module_dir"/get_schema.sql.template | sed 's/__SCHEMA__/'$dbschema'/' > "$work_dir"/get_schema.sql 
sqlplus -S ${dbuser}/${dbpass}@${dblocalid} @"$work_dir"/get_schema > "$work_dir"/${tbl_name}.schema.raw 
sqlplus -S ${dbuser}/${dbpass}@${dblocalid} @"$work_dir"/get_schema | grep -v "Name" | grep -v "\-\-\-\-"  | sed 's/NOT NULL//' | head -n -1 > "$work_dir"/${tbl_name}.schema 
awk -f "$module_dir"/build_oratble_metadata.awk "$work_dir"/${tbl_name}.schema > "$work_dir"/${tbl_name}.md.template
sed 's/__MDN__/'$model_name'/g' "$work_dir"/${tbl_name}.md.template | sed 's/__dblocalid__/'$dblocalid'/'| sed 's/__dbschema__/'${dbschema}'/' > ${model_name}.md
#psql -p $port postgres < "$work_dir"/${tbl_name}.wrap_sql >> "$work_dir"/psql_out


