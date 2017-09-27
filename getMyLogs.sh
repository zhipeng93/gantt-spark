#! /bin/bash
if [ $# -lt 5 ]
then
    echo "bash _.sh <spark_app_id> <own_log_dir> <num_cores> <num_machines> <master_node> !! [BE CAREFUL! THESE TWO SHOULD REFER TO THE SAME APPLICATION!]"
    exit
fi
cur_pwd=/Users/zhangzhipeng/Code-ETH/spark-things/code/critical-path/spark-parser
app_name=$1
own_log_dir=$2
num_cores=$3
num_machines=$4
master_node=$5

mkdir -p ghand_logs
mkdir -p ghand_logs/${app_name}

# scp ${master_node}:/mnt/ds3lab/zhanzhip/spark-event-logs/${app_name} $cur_pwd/ghand_logs/${app_name}/${app_name}
# scp ${master_node}:/mnt/local/zhipeng/$own_log_dir/task_common_log  $cur_pwd/ghand_logs/${app_name}/task_common_log
# scp ${master_node}:/mnt/local/zhipeng/$own_log_dir/task_transformation_log  $cur_pwd/ghand_logs/${app_name}/task_transformation_log
# touch $cur_pwd/ghand_logs/${app_name}/task_transformation_log

# we only analyze last several iterations in the gantt chart
python jobFilter.py $cur_pwd/ghand_logs/${app_name}/${app_name} $cur_pwd/ghand_logs/${app_name}/spark_part_log 43 52

# combine the self-generated logs into the json file.
python combineLogs.py ghand_logs/${app_name}

bash debug.sh $num_cores > ghand_logs/${app_name}/gantt.input
# cp $cur_pwd/resources/resultLogs/* $cur_pwd/ghand_logs/${app_name}
python pltGantt.py $num_machines $num_cores ghand_logs/${app_name}/gantt.input