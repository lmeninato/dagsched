import pandas as pd
from src.scheduling import Scheduler


def get_metrics_table(scheduler: Scheduler, time: int):
    if scheduler is None:
        return None
    metrics = scheduler.get_history_metrics_at_t(time)  # returns a dictionary

    data = []

    for user in scheduler.users:
        user_id, name = user["user"], user["name"]
        row = [
            name,
            len(scheduler.dags[user_id].tasks),
            scheduler.dags[user_id].arrival_time,
            metrics.get_local_preemptions(user_id),
            metrics.get_local_jct(user_id),
            metrics.get_local_queuing_time(user_id, sum),
            metrics.get_local_makespan(user_id),
        ]
        data.append(row)

    return pd.DataFrame(
        data,
        columns=[
            "User",
            "Jobs Count",
            "Arrival Time",
            "Total Preemptions",
            "Avg. Job Completion Time",
            "Job Queuing Time",
            "Makespan",
        ],
    )
