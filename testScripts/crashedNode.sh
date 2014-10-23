#!/bin/bash
#arr=(csvm0065 csvm0066 csvm0067)
arr=(mill028 mill029 mill030)
geometry=(109x24+10+40 80x24+1060+40 65x25+5-30 65x25+631-30 65x25+1258-30)
createDir="mkdir -p /work/a7109534/; rm /work/a7109534/*"
config="RMSysIntegrated.xml"
totalMessages=1000000
msgsBeforeCrash=50000
numberOfMessages=$(($totalMessages / ${#arr[@]}))
initiator=${arr[0]}
lastNode=$((${#arr[@]} - 1))
jvmArgs="-Xmx3g"
outDir="/work/a7109534/"
for ((i = 0; i < ${#arr[@]}; i++));do
   if [ $i -eq $lastNode ] && [ $(($totalMessages%${#arr[@]})) -ne 0 ]; then
       let "numberOfMessages+=1"
   fi

   command="java $jvmArgs -Djava.net.preferIPv4Stack=true -agentpath:/home/pg/p11/a7109534/yjp-2013-build-13072/bin/linux-x86-64/libyjpagent.so=monitors -jar workspace/mperf.jar -config $config -nr-messages $numberOfMessages -t-messages $totalMessages -initiator $initiator -msgs-crash $msgsBeforeCrash"
   
   if [ $i -eq $lastNode ]; then
	   command="$command -crash"
   fi
   output=" 2>&1 | tee $outDir${arr[$i]}.txt"
   gnome-terminal --geometry=${geometry[$i]} --title "${arr[$i]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[$i]}.ncl.ac.uk '$createDir; $command$output; bash'" > output.txt 2>&1
   sleep 0.5;
done
exit
