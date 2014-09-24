#!/bin/bash
#arr=(csvm0065 csvm0066 csvm0067 csvm0068)
#boxes=(csvm0067 csvm0068)
# REMEMBER TO ADD BOX MEMBERS TO AbaaS CLASS!!!!!
arr=(mill026 mill027 mill028 mill029 mill030)
boxes=(mill028 mill029 mill030)
geometry=(109x24+10+40 80x24+1060+40 65x25+5-30 65x25+631-30 65x25+1258-30)

props1="abaas_hybrid.xml"
props2="abaas_hybrid_box.xml"
channelName="uperfBox"
yourkit="-agentpath:/home/pg/p11/a7109534/yjp-2013-build-13072/bin/linux-x86-64/libyjpagent.so=monitors"
command1="java -Djava.net.preferIPv4Stack=true $yourkit -jar workspace/mperf.jar -control true -props $props1 -boxes"
command2="java -Djava.net.preferIPv4Stack=true $yourkit -jar workspace/jgroups.jar -props $props2 -channel $channelName"

for y in "${boxes[@]}"
do
    command1=$command1" "$y
done

for i in {0..4}; do

    if [[ $i < 2 ]]; then
        action=$command1;
    else
        action=$command2;
    fi

    gnome-terminal --geometry=${geometry[$i]} --title "${arr[$i]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[$i]}.ncl.ac.uk '$action; bash'"
    sleep 0.5;
done
exit
