#!/bin/bash
#arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
arr=(mill026 mill027 mill030 mill032 mill033 mill035 mill036 mill038 mill039 mill040)
geometry=109x24+10+40
outDir="workspace/output/"
props="toa_TCP.xml"
anycastRequests="true"
command="java -Djava.net.preferIPv4Stack=true -jar workspace/mperf.jar -props $props -anycastRequests $anycastRequests"
control=" -control true"

if [[ $# < 1 ]]; then
    for i in {0..9}; do
        HOST="a7109534@${arr[$i]}"
        if (( $i > 0 )); then
            ssh -T -o ConnectTimeout=1 -o StrictHostKeychecking=no  $HOST <<-ENDEXP
                nohup $command > $outDir${arr[$i]}.out 2> $outDir${arr[$i]}.err < /dev/null &
                exit
ENDEXP
        fi
    done
    $command$control;
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
