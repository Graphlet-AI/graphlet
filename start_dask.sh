#!/usr/bin/env bash

# Launch the scheduler
nohup dask scheduler --host 127.0.0.1 --port 9000 --protocol tcp --dashboard --no-show 2>&1 >> /tmp/dask.log &

# Run as many workers as we got cores
nohup nohup dask worker --nworkers=-1 tcp://127.0.0.1:9000 2>&1 >> /tmp/dask.log &
