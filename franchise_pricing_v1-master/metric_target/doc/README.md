### Crontab
#### Marking_price

0 1 0 0 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/metric_target/cron.py -env prod

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str
'-startDayOffset', help='the start-day-offset', default=0, type=int
