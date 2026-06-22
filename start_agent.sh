#!/bin/bash
# Always patch the plugin before starting agent
cd /home/prodoutbound
python3 patch_plugin.py
exec python3 agent.py start
