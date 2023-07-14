### Crontab
#### hourly pricing for om
30 11 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/om/daily.py -bo 1 -otaPricing

#30 12 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/om/daily.py -bo 2 -otaPricing

30 13 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/om/daily.py -bo 3 -otaPricing

30 14 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/om/daily.py -bo 4 -otaPricing

30 15 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/om/daily.py -bo 5 -otaPricing 

30 16 * * * /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/om/cron.py -bo 6 -otaPricing 