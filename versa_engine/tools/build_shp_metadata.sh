module_dir=$1
work_dir=$2
shp_loc=$3
tbl_name=$4
metadata_fn=$5

shp2pgsql  -p  -I ${shp_loc}  ${tbl_name} 2>/dev/null | awk -f ${dicex_basedir}/tools/shapefile_metadata.awk > ${work_dir}/${tbl_name}.schema
awk -f ${dicex_basedir}/tools/build_pgtbl_metadata.awk "$work_dir"/${tbl_name}.schema > "$work_dir"/${tbl_name}.md.template
sed 's/__TBL__/'$tbl_name'/g' "$work_dir"/${tbl_name}.md.template  > ${metadata_fn}
