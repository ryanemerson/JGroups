#!/bin/bash
arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
#arr=(mill049 mill050 mill051 mill052 mill012)
geometry=(109x24+10+40 80x24+1060+40 65x25+5-30 65x25+631-30 65x25+1258-30)
#command="java -jar -Djava.net.preferIPv4Stack=false workspace/mperf.jar -props HiTab.xml -mps 800 -mins 1"
#command="java -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -props Infinispan.xml -mps 100 -mins 1"
#command="java -Djava.net.preferIPv4Stack=true -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -props HiTab.xml -mps 800 -mins 1"
#command="java -Djava.net.preferIPv4Stack=true -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -props Infinispan.xml -mps 10 -mins 1"
#command="java -Djava.net.preferIPv4Stack=true -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -props decoupled_TOA_Box.xml"
command="java -agentpath:/home/pg/p11/a7109534/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -props Infinispan.xml"
for i in {0..4}; do
   gnome-terminal --geometry=${geometry[$i]} --title "${arr[$i]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[$i]}.ncl.ac.uk '$command; bash'"
   sleep 0.5;
done
exit
