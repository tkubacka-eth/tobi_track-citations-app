import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import random as random


from plotly.express.colors import sample_colorscale, n_colors, hex_to_rgb


def write_count_table(df, databases, count_category, cols = []):
    d = df[df['count'] == count_category][['doi'] + databases + ['median']]
    d_color = d[databases].apply(lambda x: np.sign(x - d['median']))
    d_color = d_color.replace(
        [-1, 0, 1],
        ['background-color: #fff2cc',
         'background-color: #eeeeee',
         'background-color: #d0e0e3'])
    d = d.drop('median', axis=1)
    caption_text ="""
            Colouring:
            - Blue: value is above the median.
            - Gray: value corresponds to the median.
            - Yellow: value is below the median.
            """

    if len(cols)==0:
        st.dataframe(d.style.apply(lambda _: d_color, axis=None).format(precision=0))
        st.caption(caption_text)
    else: 
        with cols[0]: 
            st.dataframe(d.style.apply(lambda _: d_color, axis=None).format(precision=0))
        with cols[1]: 
            st.caption(caption_text)
        



def plot_rel_count_sns(df, databases, count):
    d = df[df['count'] == count].set_index('doi')
    norm = d['median']
    d = d[databases].div(norm, axis=0) \
        .stack().reset_index() \
        .rename(columns={'level_1': 'database', 0: 'rel_counts'})
    f, ax = plt.subplots()
    ax = sns.stripplot(data=d, y='database', x='rel_counts', hue='doi',
                       marker='D', orient='h',
                       )
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    ax.axvline([1], ls=':', color='k')
    st.pyplot(f)

def plot_rel_count_plotly0(df, databases, count):
    all_d = df[df['count']==count].set_index('doi')
    norm = all_d['median']
    all_d = all_d[databases].div(norm, axis=0) \
        .stack().reset_index() \
        .rename(columns={'level_1': 'database', 0: 'rel_counts'})
    
    available_dois = all_d['doi'].unique()

    # colors = n_colors('#E59866', '#5DADE2', len(available_dois), colortype='rgb')
    colors = sample_colorscale('Phase', np.linspace(0,1,len(available_dois)))
    
    # fig = px.strip(all_d,
    #      x='rel_counts',
    #      y='database',
    #      color='doi',)
    fig = go.Figure()
    fig.add_trace(
        go.Box(
            y=all_d['database'],
            x=all_d['rel_counts'],
            text=all_d['doi'],
            jitter=0.5,
            boxpoints='all',
            pointpos=0,
            hoveron="points",
            fillcolor="rgba(255,255,255,0)",
            line={"color": "rgba(255,255,255,0)"},
            x0=" ",
            y0=" ",
            marker_color='black',
            marker_size=10
        ))
    fig.update_layout(
        boxmode='group')
    fig.update_traces(orientation='h')

    fig.add_vline(x=1)

    fig.update_xaxes(
        title=dict(text="count relative to median")
    )


    st.plotly_chart(fig, use_container_width=True)


def plot_rel_count_plotly(df, databases, count):
    all_d = df[df['count'] == count].set_index('doi')
    norm = all_d['median']
    all_d = all_d[databases] \
        .mask((norm > 0), all_d[databases].div(norm, axis=0)) \
        .mask((norm == 0), all_d[databases] + 1) \
        .stack().reset_index() \
        .rename(columns={'level_1': 'database', 0: 'rel_counts'})

    all_d['database_id'] = all_d['database'].apply(lambda x: len(databases)-1-databases.index(x))
    manual_jitter = 0.1

    available_dois = all_d['doi'].unique()

    # colors = n_colors('#E59866', '#5DADE2', len(available_dois), colortype='rgb')
    colors = sample_colorscale('Phase', np.linspace(0, 1, len(available_dois)))

    fig = go.Figure()
    fig.add_vline(x=1)
    for i, doi in enumerate(available_dois):
        d = all_d.query('doi == @doi')
        fig.add_trace(
            go.Box(
                y=d['database_id'] + random.sample(
                    list(np.linspace(-manual_jitter, manual_jitter, num=100)),
                    len(d['database_id'])),
                x=d['rel_counts'],
                text=d['doi'],
                jitter=0,
                boxpoints='all',
                pointpos=0,
                hoveron="points",
                hovertemplate=" count relative to median: %{x} ",
                fillcolor="rgba(255,255,255,0)",
                line={"color": "rgba(255,255,255,0)"},
                x0=" ",
                y0=" ",
                # marker_color='black',
                marker_color=colors[i],
                marker_size=11,
                name=doi
            ))
    #fig.update_layout(
    #    boxmode='group')
    fig.update_traces(orientation='h')
    fig.update_layout(yaxis_range=[min(all_d['database_id'])-manual_jitter-0.1, max(all_d['database_id'])+manual_jitter+0.1])
    fig.update_yaxes(tickmode="array", tickvals=list(range(len(databases))), ticktext=databases)
    fig.update_xaxes(
        title=dict(text="count relative to median")
    )

    st.plotly_chart(fig, use_container_width=True)