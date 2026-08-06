#!/bin/bash

# Compare accuracy of the -i argument between ping and fixed-interval pping

target_host=127.0.0.1
probe_count=10 # Minimum 2. For N probes we will only have N-1 intervals

min_interval=0.9
max_interval=1.1
interval_step=0.005

data_dir="accuracy_data"
mkdir -p $data_dir

for x in $(seq $min_interval $interval_step $max_interval); do
    echo ""
    echo "Testing rate $x with ping:"
    sudo tcpdump -i lo icmp and 'icmp[0] == 8' >$data_dir/ping_$x.txt &
    sleep 1 # Sleep before to make sure tcpdump captures the first packet
    sudo ping $target_host -q -c $probe_count -i $x
    sleep 1 # Sleep after to make sure tcpdump captures the last packet
    sudo pkill tcpdump

    echo ""
    echo "Testing rate $x with pping:"
    sudo tcpdump -i lo icmp and 'icmp[0] == 8' >$data_dir/pping_$x.txt &
    sleep 1
    sudo ./pping $target_host -z -q -c $probe_count -i $x # Use -z for fixed interval mode
    sleep 1
    sudo pkill tcpdump
done
