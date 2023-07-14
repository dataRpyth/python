### Crontab
#### Liquidation & LiquidationRelieving
40 13 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Liquidation/cron.py -jobType 1 -liquidationBatch 49 -predOcc 0.7

40 15 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Liquidation/cron.py -jobType 1 -liquidationBatch 39 -predOcc 0.75

40 18 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Liquidation/cron.py -jobType 1 -liquidationBatch 29 -predOcc 0.8

40 20 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Liquidation/cron.py -jobType 1 -liquidationBatch 29_2 -predOcc 0.9

30 20 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Liquidation/cron.py -jobType 2

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str

'-jobType', help='The type in 1:liquidation, 2:liquidation_relieving', type=int, default=1

'-liquidationBatch', help='The Batch for base_liquidation_price, 49:LIQUIDATION_BATCH_49, 39:LIQUIDATION_BATCH_39, 29:LIQUIDATION_BATCH_29', 29_2:LIQUIDATION_BATCH_29_2', type=str

'-predOcc', help='TThe predicted occ threshold for filtering hotels, type=float
