### Crontab
#### hourly pricing & liquidation
0 8-9 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1

45 9 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1 -includeOtaNonDirectHotels

0 11 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1

45 11 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1 -includeOtaNonDirectHotels

0 13-14 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1

45 14 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1 -includeOtaNonDirectHotels

0 16-17 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1

45 17-19 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1 -includeOtaNonDirectHotels

25 20 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1 -liquidationFlag 29_2 -includeOtaNonDirectHotels

0 22-23 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1

30 23 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1

#10 0 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/CHINA_Rebirth/cron.py -bo 1 -liquidationTest

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str

'-bo', help='The batch order in 1/2/3', default=1, type=int

'-otaPlugin', help='The job is running ota plugin logic or not', default=False

'-liquidationFlag', help='The env in  29_2:special_sale_29_2 ', type=str

'-includeOtaNonDirectHotels', help='The Hotels include OtaNonDirect or not', default FALSE

'-liquidationTest' help='price channel rate for liquidation or not, default FALSE

'-disablePriceDiff' help='output hotels those price same as recent price or not', default FALSE

