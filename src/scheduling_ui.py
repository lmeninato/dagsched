from dash import dcc
from scheduling import Scheduler

from dash import html


def get_scheduling_output(scheduler: Scheduler):
    if scheduler is None:
        return [
            html.Div(
                id="scheduler-error",
                children=["There was an error rendering the scheduler output"],
            )
        ]

    times = sorted(list(scheduler.history.times))

    possible_times = [{"label": "Initial", "value": -1}]

    if len(times[1:]):
        possible_times += [{"label": f"Time={i}", "value": i} for i in times[1:]]

    return [
        dcc.Dropdown(
            id="scheduling-times-dropdown",
            options=possible_times,
            value=-1,
        ),
        html.Div(id="scheduling-messages"),
        html.Div(id="scheduling-utilization"),
        html.Button(id="increase-time", n_clicks=0, children=">>"),
        html.Button(id="decrease-time", n_clicks=0, children="<<"),
    ]


def render_scheduling_messages(messages):
    return [html.P(message) for message in messages]


def render_utilization(cluster, utilization):
    cpu_usage = f"Using {utilization['cpus']} out of {cluster['cpus']} cpus"
    ram_usage = f"Using {utilization['ram']} out of {cluster['ram']} ram"
    return [html.P(cpu_usage), html.P(ram_usage)]
