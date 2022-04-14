from src.dag import DAG
from src.read_graph import read_dag_specs, parse_contents, read_yaml

# import dash
from dash import dcc

import dash_daq as daq
from dash import html
from dash_extensions.enrich import DashProxy, MultiplexerTransform, Input, Output, State

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

cyto.load_extra_layouts()

specs = read_dag_specs("data")

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
                            stylesheet=[
                                {
                                    "selector": "node",
                                    "style": {
                                        "background-color": "BFD7B5",
                                        "label": "data(label)",
                                        "text-wrap": "wrap",
                                        "text-valign": "bottom",
                                        "text-halign": "center",
                                    },
                                },
                                {
                                    "selector": "edge",
                                    "style": {"line-color": "#A3C4BC"},
                                },
                            ],
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
    pass


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
    State("session-dags", "data"),
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
        base_style = stylesheet[:2]
        style["selector"] = f"node[label = \"{data['label']}\"]"
        style["style"][
            "label"
        ] = f"{data['label']}\nDuration: {data['duration']}\nCPUs: {data['cpus']}\nRAM: {data['ram']}"  # noqa
        stylesheet = base_style + [style]

    return stylesheet


app.run_server(debug=True)
