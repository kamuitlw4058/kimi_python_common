base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
pkill -f copy_utils.py
nohup python python_common/utils/copy_utils.py &