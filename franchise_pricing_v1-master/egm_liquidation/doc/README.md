### Crontab
#### Liquidation & LiquidationRelieving
40 18 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/egm_liquidation/cron.py -jobType 1 -liquidationBatch egm_29 -predOcc 0.4 -doExclude 1 -bo 1 -env prod

40 20 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/egm_liquidation/cron.py -jobType 1 -liquidationBatch egm_29_2 -predOcc 0.4 -doSpecialBatch 1 -bo 1 -env prod

40 20 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/egm_liquidation/cron.py -jobType 1 -liquidationBatch egm_39 -predOcc 0.4 -bo 1 -env prod

40 20 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/egm_liquidation/cron.py -jobType 1 -liquidationBatch egm_49 -predOcc 0.4 -bo 1 -env prod

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str

'-jobType', help='The type in 1:liquidation, 2:liquidation_relieving', type=int, default=1

'-liquidationBatch', help='The Batch for base_liquidation_price, 49:LIQUIDATION_BATCH_49, 39:LIQUIDATION_BATCH_39, 29:LIQUIDATION_BATCH_29', 29_2:LIQUIDATION_BATCH_29_2', type=str

'-predOcc', help='TThe predicted occ threshold for filtering hotels, type=float
