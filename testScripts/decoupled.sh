#!/bin/bash
arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
boxes=(csvm0067 csvm0068)
#arr=(mill026 mill027 mill030 mill032 mill033)
#boxes=(mill032 mill033)
#arr=(mill026 mill027 mill030 mill032)
#boxes=(mill032)
geometry=(109x24+10+40 80x24+1060+40 65x25+5-30 65x25+631-30 65x25+1258-30)

anycastRequests="true"
#props1="decoupled_TOA_TCP.xml"
#props2="decoupled_TOA_Box_TCP.xml"
props1="decoupled_TOA.xml"
props2="decoupled_TOA_Box.xml"
command1="java -Djava.net.preferIPv4Stack=true -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -props $props1 -anycastRequests $anycastRequests -boxes"
command2="java -Djava.net.preferIPv4Stack=true -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/jgroups.jar -props $props2"
control=" -control true"

for y in "${boxes[@]}"
do
    command1=$command1" "$y
done

for i in {0..4}; do

    if [[ $i < 3 ]]; then
        action=$command1;
    else
        action=$command2;
        control="";
    fi

    gnome-terminal --geometry=${geometry[$i]} --title "${arr[$i]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[$i]}.ncl.ac.uk '$action$control; bash'"
    sleep 0.5;
done
exit
