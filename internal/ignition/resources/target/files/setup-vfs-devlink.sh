#!/bin/bash

echo "Calling configure-host-vfs"
/usr/local/bin/dpuagent-client.py configure-host-vfs

echo "INFO: Waiting for 10 seconds before activating devlink"
sleep 10
echo "INFO: Activating devlink"

pcie_dev_list=$(lspci -Dd 15b3: | grep ConnectX | awk '{print $1}')

for dev in ${pcie_dev_list}; do
  echo "activate devlink on dev ${dev}"
  devlink dev eswitch set pci/${dev} mode switchdev
done
echo "Finished activating devlink"
