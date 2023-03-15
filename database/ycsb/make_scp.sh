pass_file="/home/qfs/pass.txt"
while read line
do
passcode=$line
done < ${pass_file}
passshell="sshpass -p ${passcode}"
make clean
make -j
# ${passshell} ssh qfs@10.77.110.159 "cd /home/qfs/dev/gam/database/ycsb && "
# ${passshell} ssh qfs@10.77.110.159 cd /home/qfs/dev/gam/database/ycsb

${passshell} scp ycsb qfs@10.77.110.159:/home/qfs/dev-1/gam/database/ycsb
${passshell} scp ycsb qfs@10.77.110.160:/home/qfs/dev-1/gam/database/ycsb


