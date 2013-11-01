#!/bin/bash
#arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
#boxes=(csvm0067 csvm0068)
arr=(mill026 mill027 mill030 mill032 mill033)
boxes=(mill032 mill033)
geometry=109x24+10+40
outDir="workspace/output/"
anycastRequests="true"
props1="decoupled_TOA_TCP.xml"
props2="decoupled_TOA_Box_TCP.xml"
command1="java -Djava.net.preferIPv4Stack=true -jar workspace/mperf.jar -props $props1 -anycastRequests $anycastRequests -boxes"
command2="java -Djava.net.preferIPv4Stack=true -jar workspace/jgroups.jar -props $props2"
control=" -control true"

if [[ $# < 1 ]]; then
    for y in ${boxes[@]}; do
        command1=$command1" "$y
    done

    for i in {0..4}; do
    
        if [[ $i < 3 ]]; then
            action=$command1;
        else
            action=$command2;
        fi
        
        if [[ $i > 0 ]]; then
	    HOST="a7109534@${arr[$i]}"
            ssh -T -o ConnectTimeout=1 -o StrictHostKeychecking=no  $HOST <<-ENDEXP
                nohup $action > $outDir${arr[$i]}.out 2> $outDir${arr[$i]}.err < /dev/null &
                exit
ENDEXP
        else
            gnome-terminal --geometry=$geometry --title "${arr[$i]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[$i]} '$action$control; bash'";
        fi
    done
else
    if [ $1 = "kill" ]; then
       for i in ${arr[@]}; do
        HOST="a7109534@$i"
        ssh -t -o ConnectTimeout=1  $HOST 'pkill -u a7109534'
       done
    fi
fi
exit
