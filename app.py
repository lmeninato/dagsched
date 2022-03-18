from src.dag import DAG
from src.read_graph import read_dag_specs, parse_contents

import dash
from dash import dcc
from dash.dependencies import Input, Output, State
from dash import html
import dash_cytoscape as cyto
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.DEBUG,
)


app = dash.Dash(__name__)
app.config.suppress_callback_exceptions = True

cyto.load_extra_layouts()

specs = read_dag_specs("data")
elements = []  # dag.render_state()

default_stylesheet = [
    {
        "selector": "node",
        "style": {"background-color": "BFD7B5", "label": "data(label)"},
    },
    {"selector": "edge", "style": {"line-color": "#A3C4BC"}},
]


app.layout = html.Div(
    [
        html.P("DAG Scheduling Visualization"),
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
        dcc.Dropdown(
            options=list(specs.keys()),
            value=list(specs.keys())[0],
            id="input-file-dropdown",
        ),
        dcc.Dropdown(
            list(specs["simple_dag.yml"]["users"].keys()),
            list(specs["simple_dag.yml"]["users"].keys())[0],
            id="user-dropdown",
        ),
        cyto.Cytoscape(
            id="cytoscape-elements-callbacks",
            layout={
                "name": "dagre",
                # "animate": True,
                # "animationDuration": 1000,
                "rankDir": "LR",
            },
            autoRefreshLayout=True,
            stylesheet=default_stylesheet,
            style={"width": "50vw", "height": "50vh"},
            elements=elements,
        ),
    ]
)


@app.callback(
    Output("input-file-dropdown", "options"),
    Input("upload-scheduling-spec", "contents"),
    State("upload-scheduling-spec", "filename"),
    State("upload-scheduling-spec", "last_modified"),
)
def udpate_scheduling_specs(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        parsed_input_files = [
            parse_contents(c, n, d)
            for c, n, d in zip(list_of_contents, list_of_names, list_of_dates)
        ]

        for filename, spec in parsed_input_files:
            if spec is not None:
                if filename in specs:
                    logging.error(f"Error: {filename} already exists")
                else:
                    logging.info(f"Storing {filename} to specs")
                    specs[filename] = spec
            else:
                print(f"Failed to parse {filename}")

    return list(specs.keys())


@app.callback(
    Output("cytoscape-elements-callbacks", "elements"),
    [Input("input-file-dropdown", "value"), Input("user-dropdown", "value")],
)
def update_shown_dag(input_file, input_user):
    user_tasks = specs[input_file]["users"][input_user]
    dag = DAG(user_tasks)
    return dag.render_state()


app.run_server(debug=True)
