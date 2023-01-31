#!/bin/bash
set -o nounset



# With the specified arguments for benchmark setting, 
# this script runs ycsb for varied distributed ratios

# specify your hosts_file here 
# hosts_file specify a list of host names and port numbers, with the host names in the first column
hosts_file="../ycsb/config.txt"
# specify your directory for log files
#output_dir="/data/wentian"
output_dir="/home/qfs/dev/gam-logs"
#output_dir="/home/qfs/gam-swh/gam-logs"

# working environment
#proj_dir="~/programs/gam/code"
proj_dir="/home/qfs/dev/gam"
bin_dir="${proj_dir}/database/ycsb"
script_dir="{proj_dir}/database/scripts"
ssh_opts="-o StrictHostKeyChecking=no"

hosts_list=`./get_servers.sh ${hosts_file} | tr "\\n" " "`
hosts=(`echo ${hosts_list}`)
master_host=${hosts[0]}

USER_ARGS="$@"
echo "input Arguments: ${USER_ARGS}"
echo "launch..."

launch () {
  theta_ratio=$1
  output_file="${output_dir}/theta${theta_ratio}_ycsb.log"
  script="cd ${bin_dir} && ./ycsb ${USER_ARGS} -yth${theta_ratio} > ${output_file} 2>&1"
  
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
  echo "done for ${theta_ratio}" 
}

run_ycsb () {
  theta_ratios=(0.5 0.7 0.9 0.99)
  for theta_ratio in ${theta_ratios[@]}; do
    launch ${theta_ratio}
  done
}

vary_read_ratios () {
  #read_ratios=(0 30 50 70 90 100)
  read_ratios=(0) 
  for read_ratio in ${read_ratios[@]}; do
    old_user_args=${USER_ARGS}
    USER_ARGS="${USER_ARGS} -r${read_ratio}"
    run_ycsb
    USER_ARGS=${old_user_args}
  done
}

vary_temp_locality () {
  #localities=(0 30 50 70 90 100)
  localities=(0 50 100)
  for locality in ${localities[@]}; do
    old_user_args=${USER_ARGS}
    USER_ARGS="${USER_ARGS -l${locality}}"
    run_ycsb
    USER_ARGS=${old_user_args}
  done
}

auto_fill_params () {
  # so that users don't need to specify parameters for themselves
  USER_ARGS="-p11111 -sf12 -sf1 -c4 -t200000 -d0" #in ycsb -sf and -d don't work
}

auto_fill_params
run_ycsb
