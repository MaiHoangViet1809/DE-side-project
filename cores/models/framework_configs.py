from attrs import define, field, asdict
from typing import List, AnyStr, Dict, Literal
from airflow.models.param import Param
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

"""
Not yet integrated to the main code
"""

@define(slots=False)
class Params:
    run_dates: Param

    custom_kwargs: dict = None

    force_update: bool = False
    force_rerun: bool = False

    def to_dict(self):
        return asdict(self)  # noqa


@define(slots=False)
class ParamsRunDateExtend:
    start_date: str
    end_date: str
    mode: Literal["daily", "monthly", "days", "months"]
    interval: int

    def to_dict(self):
        data = asdict(self)  # noqa
        new = self.__dict__.keys()
        old = Params.__dict__.keys()
        new_keys = set(new) - set(old)
        this_configs = {k: v for k, v in data.items() if k in new}
        other_configs = {k: v for k, v in data.items() if k not in new_keys}

        return Params(run_dates=Param(default=this_configs,
                                      type=["object", "array"]),
                      **other_configs,
                      ).to_dict()


if __name__ == "__main__":
    print(ParamsRunDateExtend(start_date=(datetime.now() + relativedelta(days=-1)).strftime("%Y-%m-%d"),
                              end_date=(datetime.now() + relativedelta(days=-1)).strftime("%Y-%m-%d"),
                              mode="monthly",
                              interval=1
                              ).to_dict()
          )
