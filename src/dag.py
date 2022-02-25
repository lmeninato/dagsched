import plotly.graph_objects as go
import networkx as nx


class DAGNode:
    def __init__(self):
        pass


class DAG:
    def __init__(self, dag):
        self.graph = nx.DiGraph()
        self.name = dag["name"]

        for name, task in dag["tasks"].items():
            self.graph.add_node(name, **task)
            if "dependencies" in task:
                for dependency in task["dependencies"]:
                    self.graph.add_edge(dependency, name, color="black")


def get_figure_from_dag(G):
    """
    from networkx and dash tutorial
    """
    pos = nx.spring_layout(G)

    # edges trace
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(color="black", width=1),
        hoverinfo="none",
        showlegend=False,
        mode="lines",
    )

    # nodes trace
    node_x = []
    node_y = []
    text = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        text.append(node)

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        text=text,
        mode="markers+text",
        showlegend=False,
        hoverinfo="none",
        marker=dict(color="pink", size=50, line=dict(color="black", width=1)),
    )

    # layout
    layout = dict(
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(t=10, b=10, l=10, r=10, pad=0),
        xaxis=dict(
            linecolor="black", showgrid=False, showticklabels=False, mirror=True
        ),
        yaxis=dict(
            linecolor="black", showgrid=False, showticklabels=False, mirror=True
        ),
    )

    # figure
    return go.Figure(data=[edge_trace, node_trace], layout=layout)


if __name__ == "__main__":
    from read_graph import read_yaml

    data = read_yaml("data/simple_dag.yaml")
    tasks = data["users"]["test_user"]
    dag = DAG(tasks)
