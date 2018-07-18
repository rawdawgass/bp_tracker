#!/bin/bash
#read -p "BP :" BP
cat bp-db.txt| while read BP
do 
    echo -e "doing $BP"
    cleos -u http://api1.eosdublin.io get actions $BP -j > $BP.json
done
