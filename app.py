import dash
from dash import dcc
from dash.dependencies import Input, Output
from dash import html
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx

from src.dag import DAG, get_figure_from_dag
from src.read_graph import read_yaml


app = dash.Dash(__name__)

data = read_yaml('data/simple_dag.yaml')
tasks = data['users']['test_user']
dag = DAG(tasks)

app.layout = html.Div([
    html.P("Dash Networkx:"),
    dcc.Graph(id='dag', figure = get_figure_from_dag(dag.graph))
])

app.run_server(debug=True)
