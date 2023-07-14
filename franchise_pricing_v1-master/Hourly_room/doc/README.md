### Crontab
#### HourlyRoom

0 1 * * 1 /usr/bin/python3.6 /app/gitlab-ci/franchise_pricing_v1/master/Hourly_room/cron.py -bo 1,2 -modLen 4 -modIdx 1

### Params
'-env', help='The env in dev/test/uat/prod', default=None, type=str

'-bo', help='The batch order in 1/2/3', default='1', type=str

'-modLen', help='The model length of hash(set_oyo_id)',  default=1, type=int

'-modIdx', help='The index of the model for hash(set_oyo_id), begin with 1',  default=1, type=int
