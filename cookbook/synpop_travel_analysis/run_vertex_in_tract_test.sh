if [ $# -eq 0 ]
  then
    sessionfn=`ls -tr dbsession_*.py| tail -n 1 | sed 's/dbsession_//' | sed 's/\.py$//'`
else
    sessionfn="dbsession_"$1".py"
fi

echo $sessionfn
module_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"



versa --data_config ${module_dir}/tiger_tract_2014.xml
versa --data_config ${module_dir}/vertex_in_tract.xml 

#
python ${module_dir}/from_tiger_tract_to_radition_data.py $sessionfn
