
def plotly_concept_graph(G, file_name='concept_graph', notebook_mode=False):
    """
    Create an interactive network graph using plotly.
    Very useful for dev and debugging graph type things

    If using in a notebook, the interactive graph will be displayed in the
    notebook below the cell this method was called from.

    If called from a script, a web browser will open and the interactive graph
    will be displayed there.

    :param G: a networkx graph object containing ConceptNode nodes
    :param filename: name of file to output to
    :param notebook_mode: a boolean that must be True if called from within a
    Jupyter Notebook
    """
    import plotly
    import plotly.graph_objs as go

    import networkx as nx

    if notebook_mode:
        plotly.offline.init_notebook_mode(connected=True)

    # Generate node positions
    pos = nx.spring_layout(G)

    # Generate edge traces
    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    arrow_annotations = []
    for edge in G.edges():
        concept_node = G.node[edge[0]]['object']
        x0, y0 = pos[concept_node.key]
        concept_node = G.node[edge[1]]['object']
        x1, y1 = pos[concept_node.key]
        edge_trace['x'] += tuple([x0, x1, None])
        edge_trace['y'] += tuple([y0, y1, None])
        arrow_annotations.append(
            dict(ax=x0, ay=y0, axref='x', ayref='y',
                 x=x1, y=y1, xref='x', yref='y')
        )

    # Generate node markers
    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        textposition='top center',
        mode='markers+text',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            # colorscale options
            #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
            #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
            #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
            colorscale='Viridis',
            reversescale=True,
            color=[],
            size=20,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line=dict(width=2)))

    # Add hover labels, color nodes
    for node in G.nodes():
        x, y = pos[node]
        node_trace['x'] += tuple([x])
        node_trace['y'] += tuple([y])
        degree = nx.degree(G, node)
        node_trace['marker']['color'] += tuple([degree])
        node_trace['text'] += tuple([node.split('CONCEPT|')[-1]])

    # Generate iplot
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
        title='<br>Concept Graph',
        titlefont=dict(size=16),
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=False, zeroline=False,
                   showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False,
                   showticklabels=False),
        annotations=arrow_annotations
    )
    )

    if notebook_mode:
        plotly.offline.iplot(fig, filename=file_name)
    else:
        plotly.offline.plot(fig, filename=file_name)
