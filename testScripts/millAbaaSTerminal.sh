#!/bin/bash
#arr=(csvm0064 csvm0065 csvm0066 csvm0067 csvm0068)
#boxes=(csvm0067 csvm0068)
# REMEMBER TO ADD BOX MEMBERS TO AbaaS CLASS!!!!!
arr=(mill001 mill009 mill010 mill012 mill013 mill014 mill015 mill016 mill017 mill018 mill030 mill031 mill032)
boxes=(mill030 mill031 mill032)
#outDir="workspace/output/"
outDir="/work/a7109534/"
channelName="uperfBox"
cleanUp="mkdir -p /work/a7109534/; rm /work/a7109534/*;"
profiler="-agentpath:/home/pg/p11/a7109534/yjp-2013-build-13072/bin/linux-x86-64/libyjpagent.so=monitors"
props1="abaas_hybrid.xml"
props2="abaas_hybrid_box.xml"
#props1="abaas_TOA.xml"
#props2="abaas_TOA_Box.xml"
command1="java $profiler -Djava.net.preferIPv4Stack=true -jar workspace/mperf.jar -props $props1 -boxes"
command2="java $profiler -Djava.net.preferIPv4Stack=true -jar workspace/jgroups.jar -props $props2 -channel $channelName -nodes ${#boxes[@]}"
control=" -control true"
output="| tee $outDir'results.out'"

if [[ $# < 1 ]]; then
    for y in ${boxes[@]}; do
        command1=$command1" "$y
    done
    
    for ((i = 0; i < ${#arr[@]}; i++));do
        if (( $i < (${#arr[@]} - ${#boxes[@]}) )); then
            action=$command1;
        else
            action=$command2;
        fi
        
        if (( $i > 0 )); then
	    HOST="a7109534@${arr[$i]}"
            ssh -T -o ConnectTimeout=1 -o StrictHostKeychecking=no  $HOST <<-ENDEXP
                $cleanUp
                nohup $action > $outDir${arr[$i]}.out 2> $outDir${arr[$i]}.err < /dev/null &
                exit
ENDEXP
        fi
    done
    gnome-terminal --geometry=109x24+10+40 --title "${arr[0]}" -x bash -c "ssh -t -o ConnectTimeout=1  a7109534@${arr[0]}.ncl.ac.uk '$cleanUp$command1$control$output; bash'" > output.txt 2>&1
    sleep 0.5;
else
    if [ $1 = "kill" ]; then
       for i in ${arr[@]}; do
           if [ $i != ${arr[0]} ]; then
            HOST="a7109534@$i"
            ssh -t -o ConnectTimeout=1  $HOST 'pkill -u a7109534'
           fi
       done
       for i in ${boxes[@]}; do
            HOST="a7109534@$i"
            scp a7109534@$i:/work/a7109534/mill* .
       done
       scp a7109534@${arr[0]}:/work/a7109534/results.out .
    fi
    
    if [ $1 = "getOutput" ]; then
       rm *.err *.out output.txt
       for i in ${arr[@]}; do
            HOST="a7109534@$i"
            scp a7109534@$i:/work/a7109534/mill* .
       done
    fi
    
    if [ $1 = "getBoxOutput" ]; then
       for i in ${boxes[@]}; do
            HOST="a7109534@$i"
            scp a7109534@$i:/work/a7109534/mill* .
       done
    fi
    
    if [ $1 = "removeAllOutput" ]; then
       for i in ${arr[@]}; do
            rm $i.out $i.err
       done
    fi
    
    if [ $1 = "removeBoxOutput" ]; then
       for i in ${boxes[@]}; do
            rm $i.out $i.err
       done
    fi
fi
exit
