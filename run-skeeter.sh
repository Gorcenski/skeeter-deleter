#!/bin/bash
cd /home/$USER/docker/skeeter-deleter
docker compose up --build
docker compose down
