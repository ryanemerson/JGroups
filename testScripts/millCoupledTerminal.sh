#!/bin/bash
#arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
arr=(mill002 mill009 mill010 mill012 mill013 mill014 mill015 mill016 mill017 mill018)
geometry=109x24+10+40
outDir="/work/a7109534/"
props="toa.xml"
cleanUp="mkdir -p /work/a7109534/; rm /work/a7109534/*;"
anycastRequests="true"
command="java -Djava.net.preferIPv4Stack=true -jar workspace/mperf.jar -props $props -anycastRequests $anycastRequests"
control=" -control true"
output=" | tee $outDir'results.out'"

if [[ $# < 1 ]]; then
    for ((i = 0; i < ${#arr[@]}; i++));do
        HOST="a7109534@${arr[$i]}"
        if (( $i > 0 )); then
            ssh -T -o ConnectTimeout=1 -o StrictHostKeychecking=no  $HOST <<-ENDEXP
                mkdir $outDir
		nohup $command > $outDir${arr[$i]}.out 2> $outDir${arr[$i]}.err < /dev/null &
                exit
ENDEXP
        fi
    done
    gnome-terminal --geometry=109x24+10+40 --title "${arr[0]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[0]}.ncl.ac.uk '$cleanUp$command$control$output; bash'" > output.txt 2>&1
else
    if [ $1 = "kill" ]; then
       for i in ${arr[@]}; do
           if [ $i != ${arr[0]} ]; then
            HOST="a7109534@$i"
            ssh -t -o ConnectTimeout=1  $HOST 'pkill -u a7109534'
           fi
       done
       scp a7109534@${arr[0]}:/work/a7109534/results.out .
    fi
fi
exit
