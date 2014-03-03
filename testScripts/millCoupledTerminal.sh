#!/bin/bash
#arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
arr=(mill002 mill004 mill005 mill006 mill007 mill008 mill009 mill010 mill015 mill016)
geometry=109x24+10+40
outDir="/work/a7109534/"
props="toa.xml"
anycastRequests="true"
command="java -Djava.net.preferIPv4Stack=true -jar workspace/mperf.jar -props $props -anycastRequests $anycastRequests"
control=" -control true"

if [[ $# < 1 ]]; then
    for i in {0..9}; do
        HOST="a7109534@${arr[$i]}"
        if (( $i > 0 )); then
            ssh -T -o ConnectTimeout=1 -o StrictHostKeychecking=no  $HOST <<-ENDEXP
                mkdir $outDir
		nohup $command > $outDir${arr[$i]}.out 2> $outDir${arr[$i]}.err < /dev/null &
                exit
ENDEXP
        fi
    done
    gnome-terminal --geometry=109x24+10+40 --title "${arr[0]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[0]}.ncl.ac.uk '$command$control; bash'" > output.txt 2>&1
else
    if [ $1 = "kill" ]; then
       for i in ${arr[@]}; do
           if [ $i != ${arr[0]} ]; then
            HOST="a7109534@$i"
            ssh -t -o ConnectTimeout=1  $HOST 'pkill -u a7109534'
           fi
       done
    fi
fi
exit
