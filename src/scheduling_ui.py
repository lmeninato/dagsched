from dash import dcc
from scheduling import Scheduler

from dash import html


def generate_section_banner(title):
    return html.Div(className="section-banner", children=title)


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
        html.H5("Output Logs"),
        generate_section_banner("Select Time Instance"),
        html.Br(),
        dcc.Dropdown(
            id="scheduling-times-dropdown",
            options=possible_times,
            value=-1,
        ),
        html.Br(),
        build_output_messages(),
        # html.Button(id="increase-time", n_clicks=0, children=">>"),
        # html.Button(id="decrease-time", n_clicks=0, children="<<"),
    ]


def build_output_messages():
    return html.Div(
        id="scheduling-messages",
        style={
            "background-color": "#333399",
            "width": "100%",
            "height": "200px",
            "border": "1px solid black",
            "overflow": "scroll",
            "float": "right",
        },
    )


def render_scheduling_messages(messages):
    return [html.P(message) for message in messages]


def render_utilization(cluster, utilization):
    cpu_usage = f"Using {utilization['cpus']} out of {cluster['cpus']} cpus"
    ram_usage = f"Using {utilization['ram']} out of {cluster['ram']} ram"
    return [html.P(cpu_usage), html.P(ram_usage)]
