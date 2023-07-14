### Crontab
#### Marking_price

0 1 0 0 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Virtual_clubbing/cron.py

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str

'-bo', help='The batch order in 1/2/3', default='1', type=str