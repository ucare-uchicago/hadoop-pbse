#!/bin/bash


# download necessary tools
echo "Installing requirement..."
sudo apt-get update
sudo apt-get install -y wget git


# download hadoop binary
echo "Downloading hadoop base binary..."
wget http://apache.cs.utah.edu/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz
tar -xvzf hadoop-2.7.1.tar.gz
rm hadoop-2.7.1.tar.gz
echo "==============================="


# sparse & shallow checkout of hadoop-ucare repo
echo "Downloading hadoop configurations and scripts..."
mkdir hadoop-ucare
cd hadoop-ucare
git init
git config core.sparsecheckout true
echo "BUILDING_UCARE.txt" >> .git/info/sparse-checkout
echo "psbin/" >> .git/info/sparse-checkout
git remote add ucare-github https://github.com/ucare-uchicago/hadoop.git
git pull ucare-github ucare_se --depth=10
git checkout ucare_se
cd ..
echo "==============================="


# checkout SWIM project
echo "Downloading benchmarking scripts..."
git clone https://github.com/ucare-uchicago/SWIM.git SWIM
cd SWIM
git checkout ucare_se
cd ..
echo "==============================="

