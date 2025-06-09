from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException

from datetime import datetime
from dateutil.relativedelta import relativedelta


@task
def get_configs():
    """
    Task resolve run_dates to get list of date need to run and render params as config in first msg of pipeline

    params:
        - run_dates
    """
    from uuid import uuid4

    ctx = get_current_context()
    params = ctx["params"]
    t = ctx["task"]
    rendered_dict = {k: t.render_template(v, context=ctx) for k, v in params.items()}
    run_dates = params.get("RUN_DATES", params.get("run_dates", None))

    if run_dates:
        if type(run_dates) is list:
            pass
        elif type(run_dates) is dict:
            start_date = run_dates.get("start_date", None)
            end_date = run_dates.get("end_date", None)
            mode = run_dates.get("mode", "daily")
            interval = int(run_dates.get("interval", 1))

            if not start_date: raise AirflowFailException("[params] run_dates - missing start_date in dict")
            if not end_date: raise AirflowFailException("[params] run_dates - missing end_date in dict")

            flag_old_to_new = not (start_date > end_date)
            curr_date = datetime.strptime(start_date, '%Y-%m-%d') + relativedelta(days=1)
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            list_date = []
            while (curr_date + relativedelta(days=-1) <= end_date if flag_old_to_new
                   else curr_date + relativedelta(days=-1) >= end_date):
                list_date += [(curr_date + relativedelta(days=-1)).strftime('%Y-%m-%d')]
                curr_date += relativedelta(**{{"daily": "days", "monthly": "months"}.get(mode, mode): interval if flag_old_to_new else -interval})
            run_dates = list_date

        else:
            run_dates = [run_dates]
    else:
        run_dates = [ctx["logical_date"].strftime('%Y-%m-%d')]

    final_data = [
        [{
            "run_date": m,
            "config": rendered_dict,
            "batch_job_id": str(uuid4())
        }]
        for m in run_dates
    ]
    return final_data
