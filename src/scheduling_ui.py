from dash import dcc
from scheduling import Scheduler

from dash import html

# from src import metrics


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
            "background-color": "#3d3d5c",
            "width": "100%",
            "height": "200px",
            "border": "1px solid black",
            "overflow": "scroll",
            "float": "right",
        },
    )


def render_scheduling_messages(messages):
    return [html.P(message) for message in messages]


def generate_piechart():
    return dcc.Graph(
        id="piechart",
        figure={
            "data": [
                {
                    "labels": [],
                    "values": [],
                    "type": "pie",
                    "marker": {"line": {"color": "white", "width": 1}},
                    "hoverinfo": "label",
                    "textinfo": "label",
                }
            ],
            "layout": {
                "margin": dict(l=20, r=20, t=20, b=20),
                "showlegend": True,
                "paper_bgcolor": "rgba(0,0,0,0)",
                "plot_bgcolor": "rgba(0,0,0,0)",
                "font": {"color": "white"},
                "autosize": True,
            },
        },
    )


def render_global_metrics(cluster, metrics_t):

    queing_time = f"Queing_time:  {metrics_t.get_queuing_time()} "
    completion_time = f"Job Completion Time: {metrics_t.get_jct()} "
    makespan = f"Make-span of DAG: {metrics_t.get_makespan()} "
    return [
        # html.H4("Metrics"),
        html.P(queing_time),
        html.P(completion_time),
        html.P(makespan),
    ]


def render_user_metrics(cluster, metrics_t, users, dags):

    for user in users:
        print(user["name"])
        print(len(dags[user["user"]].tasks))

    username = f"Username:  {user['name']} "

    local_queing_time = (
        f"Local Queing Time:  {metrics_t.get_local_queuing_time(user['user'])} "
    )
    local_completion_time = (
        f" Local Job Completion Time: {metrics_t.get_local_jct(user['user'])} "
    )
    local_makespan = (
        f"Local Make-span of DAG: {metrics_t.get_local_makespan(user['user'])} "
    )

    task_count = f"Total number of jobs: { len(dags[user['user']].tasks)} "

    return [
        html.H4(" User Metrics:"),
        html.P(username),
        html.P(local_queing_time),
        html.P(local_completion_time),
        html.P(local_makespan),
        html.P(task_count),
    ]
