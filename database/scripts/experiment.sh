#!/bin/bash
set -o nounset

# With the specified arguments for benchmark setting, 
# this script runs tpcc for varied distributed ratios

# specify your hosts_file here 
# hosts_file specify a list of host names and port numbers, with the host names in the first column
hosts_file="../tpcc/config.txt"
# specify your directory for log files
#output_dir="/data/wentian"
output_dir="/home/rdmatest/dev/gam-logs"

# working environment
#proj_dir="~/programs/gam/code"
proj_dir="/home/rdmatest/dev/gam"
bin_dir="${proj_dir}/database/tpcc"
script_dir="{proj_dir}/database/scripts"
ssh_opts="-o StrictHostKeyChecking=no"

hosts_list=`./get_servers.sh ${hosts_file} | tr "\\n" " "`
hosts=(`echo ${hosts_list}`)
master_host=${hosts[0]}

USER_ARGS="$@"
echo "input Arguments: ${USER_ARGS}"
echo "launch..."

launch () {
  dist_ratio=$1
  output_file="${output_dir}/${dist_ratio}_tpcc.log"
  script="cd ${bin_dir} && ./tpcc ${USER_ARGS} -d${dist_ratio} > ${output_file} 2>&1"
  
  echo "start master: ssh ${ssh_opts} ${master_host} "$script" &"
  ssh ${ssh_opts} ${master_host} "$script" &
  sleep 3
  for ((i=1;i<${#hosts[@]};i++)); do
    host=${hosts[$i]}
    echo "start worker: ssh ${ssh_opts} ${host} "$script" &"
    ssh ${ssh_opts} ${host} "$script" &
    sleep 1
  done
  wait
  echo "done for ${dist_ratio}" 
}

run_tpcc () {
#  dist_ratios=(0 10 20 30 40 50 60 70 80 90 100)
  dist_ratios=(0 20 40 60 80 100)
  for dist_ratio in ${dist_ratios[@]}; do
    launch ${dist_ratio}
  done
}

vary_read_ratios () {
  #read_ratios=(0 30 50 70 90 100)
  read_ratios=(0)
  for read_ratio in ${read_ratios[@]}; do
    old_user_args=${USER_ARGS}
    USER_ARGS="${USER_ARGS} -r${read_ratio}"
    run_tpcc
    USER_ARGS=${old_user_args}
  done
}

vary_temp_locality () {
  #localities=(0 30 50 70 90 100)
  localities=(0 50 100)
  for locality in ${localities[@]}; do
    old_user_args=${USER_ARGS}
    USER_ARGS="${USER_ARGS -l${locality}}"
    run_tpcc
    USER_ARGS=${old_user_args}
  done
}

auto_fill_params () {
  # so that users don't need to specify parameters for themselves
  USER_ARGS="-p11111 -sf12 -sf1 -c8 -t200000"
}

launch_single_node () {
  core_cnt=$1
  output_file="${output_dir}/core_${core_cnt}_tpcc.log"
  script="cd ${bin_dir} && ./tpcc ${USER_ARGS} > ${output_file} 2>&1"
  
  echo "start master: ssh ${ssh_opts} ${master_host} "$script" &"
  ssh ${ssh_opts} ${master_host} "$script" &
  sleep 3
  for ((i=1;i<${#hosts[@]};i++)); do
    host=${hosts[$i]}
    echo "start worker: ssh ${ssh_opts} ${host} "$script" &"
    ssh ${ssh_opts} ${host} "$script" &
    sleep 1
  done
  wait
  echo "done for ${core_cnt}" 
}

run_tpcc_single_node () {
  core_cnts=(1 5 10 15 20)
  for core_cnt in ${core_cnts[@]}; do
    USER_ARGS="-p11111 -sf${core_cnt} -sf10 -c${core_cnt} -t200000"
    launch_single_node ${core_cnt}
  done
}

auto_fill_params
# run standard tpcc
run_tpcc
# launch 0

# vary_read_ratios
#vary_temp_locality

# single node, vary core count
# run_tpcc_single_node