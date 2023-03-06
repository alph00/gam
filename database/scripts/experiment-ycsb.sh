#!/bin/bash
set -o nounset



# With the specified arguments for benchmark setting, 
# this script runs ycsb for varied distributed ratios

# specify your hosts_file here 
# hosts_file specify a list of host names and port numbers, with the host names in the first column
hosts_file="../ycsb/config.txt"
# specify your directory for log files
#output_dir="/data/wentian"
output_dir="/home/rdmatest/dev/gam-logs-ycsb"
#output_dir="/home/qfs/gam-swh/gam-logs"

# working environment
#proj_dir="~/programs/gam/code"
proj_dir="/home/rdmatest/dev/gam"
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

vary_get_ratio () {
  get_ratios=(0.9 0.5) 
  put_ratios=(0 0)
  update_ratios=(0.1 0.5)
  let end=${#get_ratios[*]}-1
  for c in $(seq 0 ${end})
  do
    old_user_args=${USER_ARGS}
    USER_ARGS="${USER_ARGS} -yget${get_ratios[c]} -yput${put_ratios[c]} -yup${update_ratios[c]}"
    run_ycsb
    USER_ARGS=${old_user_args}
  done
}


vary_overall () {
  # get_ratios=(0.8 0.5 0.2) 
  # put_ratios=(0 0 0)
  # update_ratios=(0.2 0.5 0.8)
  # get_ratio_strs=(8 5 2) 
  # put_ratio_strs=(0 0 0)
  # update_ratio_strs=(2 5 8)

  get_ratios=(0.5 0.2) 
  put_ratios=(0 0)
  update_ratios=(0.5 0.8)
  get_ratio_strs=(5 2) 
  put_ratio_strs=(0 0)
  update_ratio_strs=(5 8)

  let end=${#get_ratios[*]}-1
  for c in $(seq 0 ${end})
  do
    # theta_ratios=(0.5 0.7 0.9 0.99)
    theta_ratios=(0.5 0.7)
    for theta_ratio in ${theta_ratios[@]}; do
        # core_cnts=(1 5 10 15 20)
        core_cnts=(5)
        for core_cnt in ${core_cnts[@]}; do
            launch_overall ${theta_ratio} ${core_cnt} ${get_ratios[c]} ${put_ratios[c]} ${update_ratios[c]} ${get_ratio_strs[c]} ${put_ratio_strs[c]} ${update_ratio_strs[c]} 
        done    
        # launch ${theta_ratio}
    done
  done
}

launch_overall () {
  theta_ratio=$1
  core_cnt=$2
  get_ratio=$3
  put_ratio=$4
  update_ratio=$5
  get_ratio_str=$6
  put_ratio_str=$7
  update_ratio_str=$8

  output_file="${output_dir}/theta${theta_ratio}_core${core_cnt}_get${get_ratio_str}_update${update_ratio_str}_ycsb.log"
  script="cd ${bin_dir} && ./ycsb ${USER_ARGS} -yth${theta_ratio} -c${core_cnt} -yget${get_ratio} -yput${put_ratio} -yup${update_ratio} > ${output_file} 2>&1"
  
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

auto_fill_params () {
  # so that users don't need to specify parameters for themselves
  USER_ARGS="-p11111 -sf12 -sf1 -t200000 -d0" #in ycsb -sf and -d don't work
}

auto_fill_params
#run_ycsb
# vary_get_ratio

vary_overall