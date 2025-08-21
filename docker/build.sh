#!/bin/bash
# $1 is the username

# Current directory is ~/Documents/Repositorios Bases II/Repositorio Grupal/2025-01-IC4302-PO/PO/docker
# Navigate to the downloader directory
cd downloader
docker build -t $1/downloader .
docker push $1/downloader

# Navigate back and then to s3-spider
cd ../s3-spider
docker build -t $1/s3-spider .
docker push $1/s3-spider

cd ../procesor
docker build -t $1/procesor .
docker push $1/procesor

# View running containers
docker ps