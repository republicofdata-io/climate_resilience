#!/bin/bash

# Update package list and install required build tools
sudo apt-get update

# Install g++ and other essential build tools
sudo apt-get install -y \
    build-essential \
    g++ \
    libffi-dev \
    libssl-dev \
    python3-dev
