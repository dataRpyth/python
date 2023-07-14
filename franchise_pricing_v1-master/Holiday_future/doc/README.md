### Crontab
#### HolidayFutureV1

0 8 21 8 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Holiday_future/cron.py -futureBatch 21 -holidayCode 1

0 8 28 8 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Holiday_future/cron.py -futureBatch 14 -holidayCode 1

0 8 4 9 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Holiday_future/cron.py -futureBatch 7 -holidayCode 1

0 8 9 9 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Holiday_future/cron.py -futureBatch 21 -holidayCode 2

0 8 16 9 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Holiday_future/cron.py -futureBatch 14 -holidayCode 2

0 8 23 9 * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Holiday_future/cron.py -futureBatch 7 -holidayCode 2

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str

'-futureBatch', help='The type in 7:7-days pre, 14:14-days pre, 21:21-days pre, type=str

'-holidayCode', help='The holiday code in 1: MID_AUTUMN, 2: NATIONAL_DAY', type=str