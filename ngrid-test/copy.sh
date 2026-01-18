#!/bin/bash

rsync -avz config target/ngrid*dep*.jar root@192.168.5.89:/tmp
rsync -avz config target/ngrid*dep*.jar root@192.168.5.90:/tmp
rsync -avz config target/ngrid*dep*.jar root@192.168.5.91:/tmp
