#!/bin/bash
source /home/gitlab-ci/.bash_profile

cd /app/gitlab-ci/franchise_pricing_v1/master/egm_liquidation

python3.6 cron.py  -jobType 1 -liquidationBatch egm_39 -predOcc 0.4 -bo 1 -env prod

