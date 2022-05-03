from click import style
from src.scheduling import (
    FCFS,
    PriorityScheduler,
    PreemptivePriorityScheduler,
    SmallestServiceFirst,
    ShortestJobFirst,
)
from src.scheduling_ui import (
    get_scheduling_output,
    render_global_metrics,
    render_user_metrics,
    render_scheduling_messages,
    # render_utilization,
    generate_section_banner,
    # render_global_jobcount,
    # getMetricsDF,
)


from src.dag import DAG
from src.read_graph import parse_contents, read_yaml

from dash import dcc

import dash_daq as daq
from dash import html, MATCH, ALL
from dash_extensions.enrich import DashProxy, MultiplexerTransform, Input, Output, State
from dash.exceptions import PreventUpdate


import dash_cytoscape as cyto
import logging
import glob


import os
import pathlib
import pandas as pd
import plotly.graph_objs as go


APP_PATH = str(pathlib.Path(__file__).parent.resolve())
df = pd.read_csv(os.path.join(APP_PATH, os.path.join("data", "policies.csv")))
userdf = pd.read_csv(os.path.join(APP_PATH, os.path.join("data", "users.csv")))

params = list(df)
max_length = len(df)

users = list(userdf)
suffix_row = "_row"
suffix_button_id = "_button"
suffix_sparkline_graph = "_sparkline_graph"
suffix_count = "_count"
suffix_ooc_n = "_OOC_number"
suffix_ooc_g = "_OOC_graph"
suffix_indicator = "_indicator"
suffix_test = "_testing"


logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.DEBUG,
)

# app = dash.Dash(__name__)
app = DashProxy(transforms=[MultiplexerTransform()])
app.config.suppress_callback_exceptions = True

SCHEDULER = None

cyto.load_extra_layouts()

base_cyto_stylesheet = [
    {
        "selector": "node",
        "style": {
            "background-color": "BFD7B5",  # grey
            "border-color": "#000000",  # black
            "border-width": 2,
            "border-opacity": 1,
            "label": "data(label)",
            "text-wrap": "wrap",
            "text-valign": "bottom",
            "text-halign": "center",
        },
    },
    {
        "selector": '[status = "RUNNING"]',
        "style": {
            "background-color": " #00cc99",
            "border-color": "#000000",
        },  # green #00FF00
    },
    {
        "selector": '[status = "READY"]',
        "style": {
            "background-color": "#ffcc00",
            "border-color": "#000000",
        },  # yellow #FFFF00
    },
    {
        "selector": '[status = "BLOCKED"]',
        "style": {
            "background-color": "#ff5050",
            "border-color": "#000000",
        },  # red #FF0000
    },
    {
        "selector": '[status = "FINISHED"]',
        "style": {
            "background-color": "#33cccc",
            "border-color": "#000000",
        },  # white #FFFFFF changed to blue - white difficult to understand
    },
    {
        "selector": '[status = "PREEMPTED"]',
        "style": {
            "background-color": "#9999ff",
            "border-color": "#000000",
        },  # orange   #FFA500 changed to purple
    },
    {
        "selector": "edge",
        "style": {"line-color": "#A3C4BC"},
    },
]


def build_state_legend():
    return html.Div(
        children=[
            html.H3("State Indicators"),
            html.Button(
                "RUNNING",
                style={
                    "background-color": "#00cc99",
                    "padding": "2px",
                    "margin": "2px",
                },
            ),
            html.Button(
                "READY",
                style={
                    "background-color": "#ffcc00",
                    "padding": "2px",
                    "margin": "2px",
                },
            ),
            html.Button(
                "BLOCKED",
                style={
                    "background-color": "#ff5050",
                    "padding": "2px",
                    "margin": "2px",
                },
            ),
            html.Button(
                "FINISHED",
                style={
                    "background-color": "#33cccc",
                    "padding": "2px",
                    "margin": "2px",
                },
            ),
            html.Button(
                "PREEMPTED",
                style={
                    "background-color": "#9999ff",
                    "padding": "2px",
                    "margin": "2px",
                },
            ),
        ],
        style={"padding": "20px"},
    )


def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("DAG Scheduling Visualization"),
                    html.H6("BDML Project: Spring 2022"),
                ],
            ),
        ],
    )


def build_tab1():
    return dcc.Tab(
        id="Specs-tab",
        label="Scheduling Specifications",  # label="Input Scheduling Task",
        value="tab1",
        className="custom-tab",
        selected_className="custom-tab--selected",
        children=[
            dcc.Dropdown(
                id="input-dropdown",
                options=["Sample Tasks", "Import from File", "Custom"],
                value=["Sample Tasks"],
            ),
            html.Div(id="input-ui"),
        ],
    )


def build_sched_dp():
    return html.Div(
        id="sched_pdiv",
        children=[
            html.H3("Select Policy"),
            html.Div(
                id="sched-selector",
                children=[
                    dcc.Dropdown(
                        id="scheduler-dropdown",
                        options=[
                            {"label": "First Come First Serve", "value": "FCFS"},
                            {
                                "label": "Priority Scheduler",
                                "value": "PRIO",
                            },
                            {
                                "label": "Preemptive Priority Scheduler",
                                "value": "PREPRIO",
                            },
                            {
                                "label": "Smallest Service First",
                                "value": "SSF",
                            },
                            {
                                "label": "Smallest Job First",
                                "value": "SJF",
                            },
                        ],
                        value=["FCFS"],
                    )
                ],
            ),
            build_run_btn(),
        ],
        style={
            # "width": "20%",
            "padding": "10px 10px 10px 10px",
            # "display": "inline-block",
        },
    )


def build_dag_area():
    return html.Div(
        id="cyto",
        children=[
            cyto.Cytoscape(
                id="cytoscape-elements-callbacks",
                layout={
                    "name": "dagre",
                    # "animate": True,
                    # "animationDuration": 1000,
                    "rankDir": "LR",
                },
                autoRefreshLayout=True,
                stylesheet=base_cyto_stylesheet,
                style={
                    "width": "50vw",
                    "height": "75vh",
                    "font-size": 8,
                    "margin": "25px",
                    "border": "grey solid",
                },
                elements=[],
            )
        ],
        style={
            # "float": "center",
            "padding": "0px",
            "width": "70%",
            "display": "inline-block",
            "color": "white",
        },
    )


def build_run_btn():
    return html.Div(
        id="return-btn",
        children=[html.Button(id="run-scheduler", n_clicks=0, children="Run")],
        style={"float": "right", "padding": "10px"},
    )


def build_text_output():
    return html.Div(
        id="scheduling-output-pdiv",
        children=[
            html.Hr(className="rounded"),
            html.H3("Scheduler Output"),
            html.Hr(className="rounded"),
            html.Div(
                id="scheduling-output",
            ),
        ],
        style={
            "display": "inline-block",
            "width": "20%",
            "float": "right",
            "padding": "10px",
        },
    )


def build_metrics_board():
    return html.Div(
        id="metrics-pdiv",
        children=[
            # <hr class="rounded">
            html.Hr(className="rounded"),
            html.H3("Metrics Board"),
            html.Hr(className="rounded"),
            html.Div(
                id="metric-output",
                children=[
                    build_running_stats_board(),
                ],
            ),
        ],
        style={
            "display": "inline-block",
            "width": "75%",
            "padding": "10px",
        },
    )


def build_sched_output():
    return html.Div(
        id="all-ouput",
        children=[
            build_metrics_board(),
            build_text_output(),
        ],
    )


def build_user_dp():
    return html.Div(
        id="user-dp",
        children=[
            html.H3("Select User"),
            dcc.Dropdown(id="user-dropdown", style={"color": "white"}),
        ],
        style={
            # "width": "20%",
            "padding": "10px 10px 10px 10px",
            # "display": "inline-block",
        },
    )


def build_control_buttons():
    return html.Div(
        children=[
            html.Div(children=[html.H3("Controls")]),
            html.Div(
                children=[
                    html.A(
                        html.Button(children="Start"),
                        href="https://plotly.com/get-demo/",
                        style={"padding": "2px"},
                    ),
                    html.A(
                        html.Button(children="Stop"),
                        href="https://plotly.com/get-demo/",
                        style={"padding": "2px"},
                    ),
                    html.A(
                        html.Button(children="Pause"),
                        href="https://plotly.com/get-demo/",
                        style={"padding": "2px"},
                    ),
                    html.A(
                        html.Button(children="Replay"),
                        href="https://plotly.com/get-demo/",
                        style={"padding": "2px"},
                    ),
                    html.A(
                        html.Button(children="Save"),
                        href="https://plotly.com/get-demo/",
                        style={"padding": "2px"},
                    ),
                    html.Div(
                        html.Button(
                            id="learn-more-button",
                            children="More Options",
                            n_clicks=0,
                        ),
                        style={"padding": "5px 0px"},
                    ),
                    html.Div(
                        children=[
                            html.H4("View Stages of Perfomed Scheduling"),
                            html.Button(id="decrease-time", n_clicks=0, children="<<"),
                            html.Button(id="increase-time", n_clicks=0, children=">>"),
                        ]
                    ),
                ]
            ),
        ],
        style={
            # "float": "left",
            "padding": "10px 10px 10px 10px",
            # "width": "30%",
            # "display": "inline-block",
        },
    )


def build_control_panel():
    return html.Div(
        id="control-panel",
        children=[
            html.Div(
                id="drop-downs",
                children=[build_user_dp(), build_sched_dp()],
            ),
            build_control_buttons(),
            build_state_legend(),
        ],
        style={
            # "float": "left",
            "padding": "0px",
            "width": "30%",
            "display": "inline-block",
        },
    )


def build_tab2():
    return dcc.Tab(
        id="Control-chart-tab",
        label="Visulization Dashboard",  # label="Scheduling Visualization",
        value="tab2",
        className="custom-tab",
        selected_className="custom-tab--selected",
        children=[
            build_control_panel(),
            build_dag_area(),
            build_sched_output(),
        ],
    )


def build_tabs():
    return dcc.Tabs(
        id="tabs",
        className="tabs",
        children=[build_tab1(), build_tab2()],
    )


def build_final_stats_boards():
    return html.Div(
        id="fsb",
        className="fsb",
        children=[
            html.Div(
                id="fsb-text",
                children=[
                    html.H5("FInal Summary of Scheduling Run"),
                ],
            ),
            html.Div(
                id="fsb-logo",
                children=[
                    html.A(
                        html.Button(children="Save"),
                        href="https://plotly.com/get-demo/",
                    ),
                ],
            ),
        ],
    )


def parseMetricstoCSV():
    SCHEDULER.get_history_metrics(10)
    # for every user save metrics
    # per user save time frame wise data as well.


"""Statistic Metrics"""


def init_df():
    # change this to create metrics as required
    ret = {}
    for col in list(df[1:]):
        data = df[col]
        stats = data.describe()

        std = stats["std"].tolist()
        ucl = (stats["mean"] + 3 * stats["std"]).tolist()
        lcl = (stats["mean"] - 3 * stats["std"]).tolist()
        usl = (stats["mean"] + stats["std"]).tolist()
        lsl = (stats["mean"] - stats["std"]).tolist()

        ret.update(
            {
                col: {
                    "count": stats["count"].tolist(),
                    "data": data,
                    "mean": stats["mean"].tolist(),
                    "std": std,
                    "ucl": round(ucl, 3),
                    "lcl": round(lcl, 3),
                    "usl": round(usl, 3),
                    "lsl": round(lsl, 3),
                    "min": stats["min"].tolist(),
                    "max": stats["max"].tolist(),
                    # "ooc": populate_ooc(data, ucl, lcl),
                }
            }
        )

    return ret


def populate_ooc(data, ucl, lcl):
    ooc_count = 0
    ret = []
    for i in range(len(data)):
        if data[i] >= ucl or data[i] <= lcl:
            ooc_count += 1
            ret.append(ooc_count / (i + 1))
        else:
            ret.append(ooc_count / (i + 1))
    return ret


state_dict = init_df()  # use this logic to get the metric measures


def generate_metric_row(id, style, col1, col2, col3, col4, col5):  # , col6, col7
    if style is None:
        style = {"height": "8rem", "width": "100%"}

    return html.Div(
        id=id,
        className="row metric-row",
        style=style,
        children=[
            html.Div(
                id=col1["id"],
                className="one column",
                style={"margin-right": "2.5rem", "minWidth": "50px"},
                children=col1["children"],
            ),
            html.Div(
                id=col2["id"],
                style={"textAlign": "center"},
                className="one column",
                children=col2["children"],
            ),
            html.Div(
                id=col3["id"],
                style={"height": "100%"},
                className="four columns",
                children=col3["children"],
            ),
            html.Div(
                id=col4["id"],
                style={},
                className="one column",
                children=col4["children"],
            ),
            html.Div(
                id=col5["id"],
                style={"height": "100%", "margin-top": "5rem"},
                className="three columns",
                children=col5["children"],
            ),
        ],
    )


def generate_metric_row_helper(stopped_interval, index, users):
    print(users)
    item = users[index]["user"]  # params[index]

    div_id = item + suffix_row
    button_id = item + suffix_button_id
    sparkline_graph_id = item + suffix_sparkline_graph
    count_id = item + suffix_count
    ooc_percentage_id = item + suffix_ooc_n
    ooc_graph_id = item + suffix_ooc_g
    # indicator_id = item + suffix_indicator
    # test_id = item + suffix_test

    # put metrics into df dump here
    # print(test_id)

    return generate_metric_row(
        div_id,
        None,
        {
            "id": item,
            "className": "metric-row-button-text",
            "children": html.Button(
                id=button_id,
                className="metric-row-button",
                children=item,
                title="Click to visualize live SPC chart",
                n_clicks=0,
            ),
        },  # user name
        {"id": count_id, "children": "0"},  # job count, add variables hre
        {"id": ooc_percentage_id, "children": "0.00%"},  #  job completion time
        {"id": sparkline_graph_id, "children": "0.00%"},  # job queue time
        {"id": ooc_graph_id, "children": "0"},  # makespan
    )


def build_user_stat_rows(usrcount):
    print("in buc")
    stopped_interval = 0
    divlist = []
    """if SCHEDULER:
        print("users ::", len(SCHEDULER.users))"""

    for c in range(1, usrcount + 1):
        print(c)
        divlist.append(
            generate_metric_row_helper(stopped_interval, c, users=SCHEDULER.getUsers())
        )
    # print(divlist)
    return divlist


# Build header
def generate_metric_list_header():
    return generate_metric_row(
        "metric_header",
        {"height": "3rem", "margin": "1rem 0", "textAlign": "center"},
        {"id": "m_header_1", "children": html.Div("User")},
        {"id": "m_header_2", "children": html.Div("Jobs Count")},
        # {"id": "m_header_3", "children": html.Div("Arrivals")},
        # {"id": "m_header_4", "children": html.Div("Preemptions")},
        {"id": "m_header_3", "children": html.Div("Job Completion Time")},
        {"id": "m_header_4", "children": html.Div("Job Queue Time")},
        {"id": "m_header_5", "children": html.Div("Make-Span")},
    )


def build_top_panel(stopped_interval):
    return html.Div(
        id="top-section-container",
        className="row",
        children=[
            # Metrics summary
            html.Div(
                id="metric-summary-session",
                className="eight columns",
                children=[
                    generate_section_banner(
                        "Schedule Policy Execution Metrics Summary"
                    ),
                    html.Div(
                        id="metric-div",
                        children=[
                            generate_metric_list_header(),
                            html.Div(id="metric-rows"),
                        ],
                    ),
                ],
                style={"width": "60%"},
            ),
            # Piechart
            html.Div(
                id="ooc-piechart-outer",
                className="four columns",
                children=[
                    generate_section_banner("Global Summary"),
                    # next div below
                    html.Div(
                        id="gbleft",
                        children=[
                            html.Div(id="scheduling-jobcount"),
                            html.Div(id="scheduling-utilization"),
                        ],
                        style={"float": "left", "width": "50%"},
                    ),
                    html.Div(
                        id="gbrigth",
                        children=[
                            html.Div(id="scheduling-metrics"),
                        ],
                        style={"float": "right", "width": "50%"},
                    ),
                ],
                style={"width": "35%"},
            ),
        ],
    )


def generate_ledbox(title, value):
    return html.Div(
        id="quick-stats",
        className="row",
        children=[
            html.Div(
                id="test",
                children=[
                    html.H2(title, style={"font-size": "14px"}),  #
                    daq.LEDDisplay(
                        id="operator-led",
                        value=value,
                        color="#92e0d3",
                        backgroundColor="#1e2130",
                        size=10,
                    ),
                ],
            ),
        ],
    )  # div ends


def render_global_jobcount(dags, users):
    jobcount = 0
    for user in users:
        jobcount += len(dags[user["user"]].tasks)
    return generate_ledbox("Global Job Count", jobcount)


def render_utilization(cluster, utilization):
    cpuusgperc = (utilization["cpus"] / cluster["cpus"]) * 100
    ramusgperc = (utilization["ram"] / cluster["ram"]) * 100
    cpu_usage = f"Using {utilization['cpus']} out of { cluster['cpus']} cpus"
    ram_usage = f"Using {utilization['ram']} out of {cluster['ram']} ram"
    return [
        generate_ledbox("CPU utilization (%)", cpuusgperc),
        # html.P(cpu_usage),
        generate_ledbox("RAM utilization (%)", ramusgperc),
        # html.P(ram_usage),
    ]


def build_running_stats_board():

    return html.Div(
        id="rsb",
        className="rsb",
        children=[
            html.Div(
                id="rsb-text",
                children=[
                    html.H5("Running Statistics"),
                ],
            ),
            html.Div(
                id="rsb-logo",  # save local and global stats with a single button
                children=[
                    build_top_panel(1),
                    html.Div(id="scheduling-user-metrics"),
                    html.Div(
                        children=[
                            html.A(
                                html.Button(children="Save"),
                                href="https://plotly.com/get-demo/",
                            ),
                        ],
                        style={"float": "right"},
                    ),
                ],
            ),
        ],
    )


""" Stats Logic Ends"""


app.layout = html.Div(
    [
        # store current list of DAGs
        dcc.Store(id="session-cluster", storage_type="session"),
        dcc.Store(id="session-dags", storage_type="session"),
        dcc.Store(id="session-users", storage_type="session"),
        build_banner(),
        build_tabs(),
    ]
)


@app.callback(
    Output("scheduling-output", "children"),
    Output("metric-rows", "children"),
    Input("run-scheduler", "n_clicks"),
    [State("scheduler-dropdown", "value")],
    State("session-dags", "data"),
    State("session-users", "data"),
    State("session-cluster", "data"),
    prevent_initial_call=True,
)
def perform_scheduling(n_clicks, scheduler_type, dags, users, cluster):
    global SCHEDULER

    if isinstance(scheduler_type, list):
        scheduler_type = scheduler_type[0]
    try:
        if scheduler_type == "FCFS":
            SCHEDULER = FCFS(cluster, dags, users)
        elif scheduler_type == "PRIO":
            SCHEDULER = PriorityScheduler(cluster, dags, users)
        elif scheduler_type == "SSF":
            SCHEDULER = SmallestServiceFirst(cluster, dags, users)
        elif scheduler_type == "PREPRIO":
            SCHEDULER = PreemptivePriorityScheduler(cluster, dags, users)
        elif scheduler_type == "SJF":
            SCHEDULER = ShortestJobFirst(cluster, dags, users)
        else:
            logging.error(f"Invalid scheduler selected: {scheduler_type}")
            raise ValueError

        if SCHEDULER:
            SCHEDULER.run()
    except Exception:
        SCHEDULER = None

    return (
        get_scheduling_output(SCHEDULER),
        build_user_stat_rows(len(SCHEDULER.users) - 1),
    )  # one less than users count


@app.callback(
    Output("scheduling-user-metrics", "children"),
    Input("scheduling-times-dropdown", "value"),
    prevent_initial_call=True,
)
def render_state_from_scheduler_history_per_user(time):
    logging.info(f"Selected time is {time}")
    if SCHEDULER is None:
        return None
    users = SCHEDULER.getUsers()
    metrics_t = SCHEDULER.get_history_metrics_at_t(time)  # returns a dictionary

    return render_user_metrics(SCHEDULER.cluster, metrics_t, users, SCHEDULER.dags)


@app.callback(
    Output("session-dags", "data"),
    Output("scheduling-messages", "children"),
    Output("scheduling-utilization", "children"),
    Output("scheduling-metrics", "children"),
    Output("scheduling-jobcount", "children"),
    Input("scheduling-times-dropdown", "value"),
    State("session-dags", "data"),
    prevent_initial_call=True,
)
def render_state_from_scheduler_history(time, dags):
    logging.info(f"Selected time is {time}")

    if SCHEDULER is None:
        return dags, None, None

    messages, dags, utilization = SCHEDULER.get_history(time)
    metrics_t = SCHEDULER.get_history_metrics_at_t(time)  # returns a dictionary

    return (
        dags,
        render_scheduling_messages(messages),
        render_utilization(SCHEDULER.cluster, utilization),
        render_global_metrics(SCHEDULER.cluster, metrics_t),
        render_global_jobcount(SCHEDULER.dags, SCHEDULER.getUsers()),
    )


@app.callback(
    Output("scheduling-times-dropdown", "value"),
    Input("increase-time", "n_clicks"),
    State("scheduling-times-dropdown", "options"),
    State("scheduling-times-dropdown", "value"),
    prevent_initial_call=True,
)
def increase_history_time(n_clicks, options, time):
    values = [option["value"] for option in options]
    index = values.index(time)
    return values[(index + 1) % len(options)]


@app.callback(
    Output("scheduling-times-dropdown", "value"),
    Input("decrease-time", "n_clicks"),
    State("scheduling-times-dropdown", "options"),
    State("scheduling-times-dropdown", "value"),
    prevent_initial_call=True,
)
def decrease_history_time(n_clicks, options, time):
    values = [option["value"] for option in options]
    index = values.index(time)
    return values[(index - 1) % len(options)]


@app.callback(Output("input-ui", "children"), [Input("input-dropdown", "value")])
def render_input_ui(value):
    logging.info(f"selected input type: {value}")
    if isinstance(value, list):
        input_option = value[0]
    else:
        input_option = value
    if input_option == "Import from File":
        # "Import from File" is checked -> render ui to import file:
        return [
            dcc.Upload(
                id="upload-scheduling-spec",
                children=html.Div(["Drag and Drop or ", html.A("Select Files")]),
                style={
                    "width": "25%",
                    # "height": "60px",
                    # "lineHeight": "60px",
                    "borderWidth": "1px",
                    "borderStyle": "dashed",
                    "borderRadius": "5px",
                    "textAlign": "center",
                    "margin": "10px",
                },
                # Allow multiple files to be uploaded
                multiple=True,
            ),
        ]

    elif input_option == "Sample Tasks":
        return [
            dcc.Dropdown(
                id="sample-file-dropdown",
                options=glob.glob("data/*.yml"),
            ),
        ]
    # else create custom scheduling tasks data
    return [
        html.H1("Input Specifications"),
        html.H6("Enter Number of Users:"),
        daq.NumericInput(id="input-number-users", value=1, className="numeric-input"),
        html.Div(id="custom-user-tasks-ui"),
    ]


@app.callback(
    Output("custom-user-tasks-ui", "children"), Input("input-number-users", "value")
)
def render_custom_user_tasks_ui(num_users):
    logging.info("todo: render custom user tasks UI")

    ui_elements = [
        daq.NumericInput(
            id="input-cluster-cpus",
            label="Input Number of CPUs",
            value=10,
            className="numeric-input",
        ),
        daq.NumericInput(
            id="input-cluster-ram",
            label="Input GBs of RAMs",
            value=20,
            className="numeric-input",
        ),
    ]

    for i in range(num_users):
        user_task_i = create_user_task(i)
        ui_elements += user_task_i

    return ui_elements + [
        html.Button("Submit", id="submit-custom-dag", n_clicks=0),
    ]


@app.callback(
    Output("session-cluster", "data"),
    Output("session-dags", "data"),
    Output("session-users", "data"),
    [
        Input("submit-custom-dag", "n_clicks"),
        Input("input-cluster-cpus", "value"),
        Input("input-cluster-ram", "value"),
    ],
    State({"type": "input-user-name", "user": ALL}, "value"),
    State({"type": "input-arrival", "user": ALL}, "value"),
    State({"type": "input-num-tasks", "user": ALL}, "value"),
    State({"type": "input-task", "subtype": ALL, "user": ALL, "task": ALL}, "value"),
    prevent_initial_call=True,
)
def handle_submit_custom_dag(n_clicks, cpus, ram, users, arrivals, tasks, task_form):
    if n_clicks is None or n_clicks < 1:
        raise PreventUpdate

    logging.info("submit custom dag triggered!")
    logging.info(f"n_clicks: {n_clicks}")
    logging.info(f"user_name: {users}")
    logging.info(f"arrival: {arrivals}")
    logging.info(f"num_tasks: {tasks}")
    logging.info(f"task_form: {task_form}")

    if len(task_form) != (sum(tasks) * 4):
        logging.error(f"Error with task form: {task_form}")
        raise PreventUpdate
    index = 0

    cluster = {"cpus": cpus, "ram": ram}
    data = {}
    cluster_users = []

    for user, arrival, num_tasks in zip(users, arrivals, tasks):
        index, data[user] = parse_tasks(index, user, arrival, num_tasks, task_form)
        cluster_users.append({"user": user, "name": user})

    return cluster, data, cluster_users


def parse_tasks(i, user, arrival, num_tasks, task_form):
    dag = {"name": user, "arrival_time": arrival, "tasks": {}}

    for j in range(num_tasks):
        form_elements = task_form[i : (i + 4)]
        i += 4

        duration, cpus, ram, deps = form_elements
        dag["tasks"][f"task_{j}"] = {
            "label": f"task_{j}",
            "duration": duration,
            "cpus": cpus,
            "ram": ram,
            "dependencies": deps,
        }

    dag = DAG(dag)
    return i, dag


@app.callback(
    Output({"type": "user-tasks-form", "user": MATCH}, "children"),
    Input({"type": "input-num-tasks", "user": MATCH}, "value"),
    State({"type": "user-tasks-form", "user": MATCH}, "id"),
)
def create_user_task_callbacks(num_tasks, user_form):
    user_id = user_form["user"]
    logging.info(f"Creating {num_tasks} tasks for user {user_id}")

    result = []
    for task_num in range(num_tasks):
        ui = html.Div(
            [
                html.H2(f"Enter details for task_{task_num}:"),
                daq.NumericInput(
                    id={
                        "type": "input-task",
                        "subtype": "duration",
                        "user": user_id,
                        "task": task_num,
                    },
                    label="Input Task Duration",
                    value=5,
                    className="numeric-input",
                ),
                daq.NumericInput(
                    id={
                        "type": "input-task",
                        "subtype": "cpus",
                        "user": user_id,
                        "task": task_num,
                    },
                    label="Input Task Required CPUs",
                    value=1,
                    className="numeric-input",
                ),
                daq.NumericInput(
                    id={
                        "type": "input-task",
                        "subtype": "ram",
                        "user": user_id,
                        "task": task_num,
                    },
                    label="Input Task Required RAM",
                    value=1,
                    className="numeric-input",
                ),
                dcc.Dropdown(
                    id={
                        "type": "input-task",
                        "subtype": "dependencies",
                        "user": user_id,
                        "task": task_num,
                    },
                    options=[f"task_{i}" for i in range(task_num)],
                    placeholder="Please Select Task Dependencies:",
                    multi=True,
                    className="numeric-input",
                ),
            ]
        )
        result.append(ui)
    return result


def create_user_task(i):
    return [
        html.H3("User " + str(i) + "'s Specifications"),
        dcc.Input(
            id={"type": "input-user-name", "user": i},
            type="text",
            placeholder="Input User Name:",
        ),
        daq.NumericInput(
            id={"type": "input-arrival", "user": i},
            label="Input Arrival Time:",
            value=0,
            className="numeric-input",
        ),
        daq.NumericInput(
            id={"type": "input-num-tasks", "user": i},
            label=f"Input Number of tasks for User {i}",
            value=2,
            className="numeric-input",
        ),
        html.Div(id={"type": "user-tasks-form", "user": i}),
    ]


@app.callback(
    Output("user-dropdown", "options"),
    Output("user-dropdown", "value"),
    [Input("session-users", "modified_timestamp")],
    State("session-users", "data"),
)
def get_users_in_session(time_stamp, data):
    if data:
        users = [user["name"] for user in data]
        users = ["All Users"] + users
        return users, users[0]
    return None, None


@app.callback(
    Output("session-cluster", "data"),
    Output("session-dags", "data"),
    Output("session-users", "data"),
    Input("upload-scheduling-spec", "contents"),
    State("upload-scheduling-spec", "filename"),
    State("upload-scheduling-spec", "last_modified"),
)
def handle_file_upload(list_of_contents, list_of_names, list_of_dates):
    logging.info("handling upload")
    cluster = {}
    data = {}
    users = []

    if list_of_contents is not None:
        parsed_input_files = [
            parse_contents(c, n, d)
            for c, n, d in zip(list_of_contents, list_of_names, list_of_dates)
        ]

        for filename, spec in parsed_input_files:
            if spec is not None:
                logging.info(f"Storing {filename} to session-dags")
                cluster = spec["cluster"]
                for user, tasks in spec["users"].items():
                    data[user] = DAG(tasks)
                    name = data[user].name
                    users.append({"user": user, "name": name})
            else:
                logging.error(f"Failed to parse {filename}")
    return cluster, data, users


@app.callback(
    Output("session-cluster", "data"),
    Output("session-dags", "data"),
    Output("session-users", "data"),
    Input("sample-file-dropdown", "value"),
)
def update_scheduling_tasks_from_sample(path):
    cluster = {}
    data = {}
    users = []

    if not path:
        return None, None, None

    try:
        raw_data = read_yaml(path)
        cluster = raw_data["cluster"]
        for user, tasks in raw_data["users"].items():
            data[user] = DAG(tasks)
            name = data[user].name
            users.append({"user": user, "name": name})
    except Exception as e:
        logging.error(f"error: {e}")
        logging.error(f"Error reading sample file {path}")

    return cluster, data, users


@app.callback(
    Output("cytoscape-elements-callbacks", "elements"),
    Input("user-dropdown", "value"),
    Input("session-dags", "data"),
    State("session-users", "data"),
    prevent_initial_call=True,
)
def update_shown_dag(value, dags, users):
    index = None
    if not users:
        return []
    if value == "All Users":
        elements = []
        for _, dag in dags.items():
            current_dag = DAG(dag, deserialize=True)
            elements += current_dag.render_state()
        return elements
    for i, user in enumerate(users):
        if user["name"] == value:
            index = i
            break
    if index is None:
        return []
    user = users[index]["user"]
    dag = DAG(dags[user], deserialize=True)
    return dag.render_state()


@app.callback(
    Output("cytoscape-elements-callbacks", "stylesheet"),
    [
        Input("cytoscape-elements-callbacks", "stylesheet"),
        Input("cytoscape-elements-callbacks", "mouseoverNodeData"),
    ],
)
def displayMouseOverNodeData(stylesheet, data):
    style = {
        "selector": "node",
        "style": {
            "background-color": "BFD7B5",
            "label": "data(label)",
            "text-wrap": "wrap",
            "text-valign": "bottom",
            "text-halign": "center",
            "border-color": "#ff9900",
            "border-width": 2,
            "border-opacity": 1,
            "color": "#ff9900",
            "text-opacity": 1,
            "font-size": 8,
            "z-index": 9999,
        },
    }

    if not data or "parent" not in data:
        # no need to style compound nodes on hover:
        return stylesheet

    base_stylesheet = base_cyto_stylesheet
    style["selector"] = f"node[id = \"{data['id']}\"]"
    style["style"][
        "label"
    ] = f"{data['label']}\nDuration: {data['duration']}\nCPUs: {data['cpus']}\nRAM: {data['ram']}"  # noqa

    stylesheet = base_stylesheet + [style]

    return stylesheet


app.run_server(debug=True)
