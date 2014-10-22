#!/bin/bash
arr=(mill001 mill004 mill005 mill007 mill009 mill010 mill011 mill013 mill014 mill017 mill019 mill020 mill021 mill024 mill026 mill027 mill028 mill029 mill031 mill032 mill035)
#arr=(mill001 mill002 mill004 mill006 mill007 mill008)
geometry=109x26+3476+-8
outDir="/work/a7109534/"
props="ProbingValidation.xml"
probingNodes=5
numberOfRounds=400
roundDuration=1000
cleanUp="mkdir -p /work/a7109534/; rm /work/a7109534/*;"
command="java -Djava.net.preferIPv4Stack=true -jar workspace/probingValidation.jar -p $props -pn $probingNodes -rd $roundDuration -nr $numberOfRounds"
master=" -m"
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
    gnome-terminal --geometry=109x24+10+40 --title "${arr[0]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[0]}.ncl.ac.uk '$cleanUp$command$master$output; bash'" > output.txt 2>&1
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
