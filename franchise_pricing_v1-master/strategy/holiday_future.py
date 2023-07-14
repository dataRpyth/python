from sympy import Interval


class HolidayFutureStrategy:
    v1_base_rate_strategy_map = {
        "7": [
            {
                "occ": Interval(0, 0.3),
                "rate_diff": -0.1
            }, {
                "occ": Interval(0.3, 0.4, left_open=True),
                "rate_diff": -0.05
            }, {
                "occ": Interval(0.4, 0.5, left_open=True),
                "rate_diff": 0
            }, {
                "occ": Interval(0.5, 0.6, left_open=True),
                "rate_diff": 0.1
            }
        ],
        "14": [
            {
                "occ": Interval(0, 0.2),
                "rate_diff": -0.1
            }, {
                "occ": Interval(0.2, 0.3, left_open=True),
                "rate_diff": -0.05
            }, {
                "occ": Interval(0.3, 0.4, left_open=True),
                "rate_diff": 0
            }, {
                "occ": Interval(0.4, 0.5, left_open=True),
                "rate_diff": 0.1
            }
        ],
        "21": [
            {
                "occ": Interval(0, 0.1),
                "rate_diff": -0.1
            }, {
                "occ": Interval(0.1, 0.2, left_open=True),
                "rate_diff": -0.05
            }, {
                "occ": Interval(0.2, 0.3, left_open=True),
                "rate_diff": 0
            }, {
                "occ": Interval(0.3, 0.4, left_open=True),
                "rate_diff": 0.1
            }
        ]
    }

    @staticmethod
    def get_rate_diff(batch, occ):
        batch = HolidayFutureStrategy.v1_base_rate_strategy_map.get(batch)
        for item in batch:
            if occ in item.get("occ"):
                return item.get("rate_diff")
        return 0

    @staticmethod
    def get_base_ratio(batch, base, occ):
        return base * (1 + HolidayFutureStrategy.get_rate_diff(batch, occ))

    v1_holiday_rate_strategy_map = {
        "1":
            [{
                "date": "09-12",
                "rate": 0.9,

            }, {
                "date": "09-13",
                "rate": 1,

            }, {
                "date": "09-14",
                "rate": 1.1,

            }, {
                "date": "09-15",
                "rate": 0.9,

            }]
        ,
        "2":
            [{
                "date": "09-30",
                "rate": 0.9,

            }, {
                "date": "10-01",
                "rate": 0.95,

            }, {
                "date": "10-02",
                "rate": 1,

            }, {
                "date": "10-03",
                "rate": 1.1,

            }, {
                "date": "10-04",
                "rate": 1.25,

            }, {
                "date": "10-05",
                "rate": 1,

            }, {
                "date": "10-06",
                "rate": 0.95,

            }, {
                "date": "10-07",
                "rate": 0.9,

            }]
    }

    @staticmethod
    def get_holiday_ratio(holiday_code, i, key=None):
        if key is None:
            return HolidayFutureStrategy.v1_holiday_rate_strategy_map.get(holiday_code)[i]
        return HolidayFutureStrategy.v1_holiday_rate_strategy_map.get(holiday_code)[i].get(key)

    @staticmethod
    def get_holiday_via_ratio(holiday_code, base, day):
        batch_rate = HolidayFutureStrategy.v1_holiday_rate_strategy_map.get(holiday_code)
        return base * (batch_rate.get(day))
