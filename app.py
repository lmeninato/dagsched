from src.scheduling import FCFS
from src.scheduling_ui import (
    get_scheduling_output,
    render_scheduling_messages,
    render_utilization,
)
from src.dag import DAG
from src.read_graph import read_dag_specs, parse_contents, read_yaml

from dash import dcc

import dash_daq as daq
from dash import html, MATCH, ALL
from dash_extensions.enrich import DashProxy, MultiplexerTransform, Input, Output, State
from dash.exceptions import PreventUpdate


import dash_cytoscape as cyto
import logging
import glob

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

specs = read_dag_specs("data")

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
        "style": {"background-color": "#00FF00", "border-color": "#000000"},  # green
    },
    {
        "selector": '[status = "READY"]',
        "style": {"background-color": "#FFFF00", "border-color": "#000000"},  # yellow
    },
    {
        "selector": '[status = "BLOCKED"]',
        "style": {"background-color": "#FF0000", "border-color": "#000000"},  # red
    },
    {
        "selector": '[status = "FINISHED"]',
        "style": {"background-color": "#FFFFFF", "border-color": "#000000"},  # white
    },
    {
        "selector": "edge",
        "style": {"line-color": "#A3C4BC"},
    },
]

app.layout = html.Div(
    [
        html.P("DAG Scheduling Visualization"),
        # store current list of DAGs
        dcc.Store(id="session-cluster", storage_type="session"),
        dcc.Store(id="session-dags", storage_type="session"),
        dcc.Store(id="session-users", storage_type="session"),
        dcc.Tabs(
            id="tabs",
            children=[
                dcc.Tab(
                    label="Input Scheduling Task",
                    children=[
                        dcc.Dropdown(
                            id="input-dropdown",
                            options=["Sample Tasks", "Import from File", "Custom"],
                            value=["Sample Tasks"],
                        ),
                        html.Div(id="input-ui"),
                    ],
                ),
                dcc.Tab(
                    label="Scheduling Visualization",
                    children=[
                        dcc.Dropdown(
                            id="scheduler-dropdown", options=["FCFS"], value=["FCFS"]
                        ),
                        html.Button(id="run-scheduler", n_clicks=0, children="Run"),
                        html.Div(id="scheduling-output"),
                        dcc.Dropdown(id="user-dropdown"),
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
                        ),
                    ],
                ),
            ],
        ),
    ]
)


@app.callback(
    Output("scheduling-output", "children"),
    Input("run-scheduler", "n_clicks"),
    State("scheduler-dropdown", "value"),
    State("session-dags", "data"),
    State("session-users", "data"),
    State("session-cluster", "data"),
    prevent_initial_call=True,
)
def perform_scheduling(n_clicks, scheduler_type, dags, users, cluster):
    global SCHEDULER

    scheduler_type = scheduler_type[0]
    try:
        if scheduler_type == "FCFS":
            SCHEDULER = FCFS(cluster, dags, users)
        else:
            logging.error("Invalid scheduler selected")
            raise ValueError

        if SCHEDULER:
            SCHEDULER.run()
    except Exception:
        SCHEDULER = None

    return get_scheduling_output(SCHEDULER)


@app.callback(
    Output("session-dags", "data"),
    Output("scheduling-messages", "children"),
    Output("scheduling-utilization", "children"),
    Input("scheduling-times-dropdown", "value"),
    State("session-dags", "data"),
    prevent_initial_call=True,
)
def render_state_from_scheduler_history(time, dags):
    logging.info(f"Selected time is {time}")

    if SCHEDULER is None:
        return dags, None, None

    messages, dags, utilization = SCHEDULER.get_history(time)

    return (
        dags,
        render_scheduling_messages(messages),
        render_utilization(SCHEDULER.cluster, utilization),
    )


@app.callback(Output("input-ui", "children"), [Input("input-dropdown", "value")])
def render_input_ui(value):
    input_option = value[0]
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
        html.H1("todo - render UI for custom DAG"),
        daq.NumericInput(id="input-number-users", value=1),
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
            label="Input Number of CPUs:",
            value=10,
        ),
        daq.NumericInput(
            id="input-cluster-ram",
            label="Input GBs of RAMs:",
            value=20,
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
                    label="Input Task Duration:",
                    value=5,
                ),
                daq.NumericInput(
                    id={
                        "type": "input-task",
                        "subtype": "cpus",
                        "user": user_id,
                        "task": task_num,
                    },
                    label="Input Task Required CPUs:",
                    value=1,
                ),
                daq.NumericInput(
                    id={
                        "type": "input-task",
                        "subtype": "ram",
                        "user": user_id,
                        "task": task_num,
                    },
                    label="Input Task Required RAM:",
                    value=1,
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
                ),
            ]
        )
        result.append(ui)
    return result


def create_user_task(i):
    return [
        dcc.Input(
            id={"type": "input-user-name", "user": i},
            type="text",
            placeholder="Input User Name:",
        ),
        daq.NumericInput(
            id={"type": "input-arrival", "user": i},
            label="Input Arrival Time:",
            value=0,
        ),
        daq.NumericInput(
            id={"type": "input-num-tasks", "user": i},
            label=f"Input Number of tasks for User {i}",
            value=2,
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
)
def update_shown_dag(value, dags, users):
    index = None
    if not users:
        return []
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
def displayTapNodeData(stylesheet, data):
    style = {
        "selector": "node",
        "style": {
            "background-color": "BFD7B5",
            "label": "data(label)",
            "text-wrap": "wrap",
            "text-valign": "bottom",
            "text-halign": "center",
            "border-color": "purple",
            "border-width": 2,
            "border-opacity": 1,
            "color": "#B10DC9",
            "text-opacity": 1,
            "font-size": 8,
            "z-index": 9999,
        },
    }

    if data:
        base_stylesheet = base_cyto_stylesheet
        style["selector"] = f"node[label = \"{data['label']}\"]"
        style["style"][
            "label"
        ] = f"{data['label']}\nDuration: {data['duration']}\nCPUs: {data['cpus']}\nRAM: {data['ram']}"  # noqa

        stylesheet = base_stylesheet + [style]

    return stylesheet


app.run_server(debug=True)
