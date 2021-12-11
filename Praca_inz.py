#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import dash
from dash import  dcc
from dash import  html
from dash import dash_table, callback_context
from dash.dependencies import Input, Output, State
import plotly.express as px
import base64
import datetime
import io
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# In[2]:


cases = pd.read_csv("https://covid.ourworldindata.org/data/owid-covid-data.csv");
# cases = pd.read_csv("covid_data.csv");
cases['date'] = pd.to_datetime(cases['date'], utc=False).dt.date


# In[3]:


mobility = pd.read_csv("https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv");
#mobility = pd.read_csv("google_mobility.csv");
mobility['date'] = pd.to_datetime(mobility['date'], utc=False).dt.date


# In[4]:


data_frames= {"mobility": mobility, "cases" : cases}


# In[5]:


def merge(df1, df2, left_key, right_key, merge_type):
    df_merged = pd.merge(df1, df2, how=merge_type, left_on=left_key, right_on=right_key)
    return df_merged


# In[6]:


def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df_uploaded = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df_uploaded = pd.read_excel(io.BytesIO(decoded))
        elif 'json' in filename:
            # Assume that the user uploaded a JSON file
            df_uploaded = pd.read_json(
                io.StringIO(decoded.decode('utf-8')))
        else:
            return dcc.Markdown(
            'There was an error processing this file! - this format is not supported'
        )
    except Exception as e:
        return dcc.Markdown(
            'There was an error processing this file!'
        )
    #unify date format if there is column like "date"
    for column_name in df_uploaded.columns:
        if 'date' in column_name or 'Date' in column_name or 'DATE' in column_name:
            df_uploaded[column_name] = pd.to_datetime(df_uploaded[column_name], utc=False).dt.date

    data_frames[filename] = df_uploaded
    df_filtered = df_uploaded.head(20)
    return html.Div(dash_table.DataTable(
                    id='datatable-paging-upload',
                    data = df_filtered.to_dict('records'),
                    columns=[{"name": i, "id": i} for i in df_filtered.columns],
                    page_current=0,
                    page_size=PAGE_SIZE,
                    page_action='custom',
                    style_table={'overflowX': 'scroll'}
                    )
                   )


# In[ ]:


app = dash.Dash(__name__, title='Data integration')

PAGE_SIZE = 20

app.layout = html.Div([
        dcc.Tabs([
             dcc.Tab(label = 'upload',
                children=[
                    dcc.Upload(
                    id='upload-data',
                    children=html.Div([
                        'Drag and Drop or ',
                        html.A('Select Files')
                        ]),
                    style={
                        'width': '100%',
                        'height': '120px',
                        'lineHeight': '60px',
                        'borderWidth': '1px',
                        'borderStyle': 'dashed',
                        'borderRadius': '5px',
                        'textAlign': 'center',
                        'margin': '10px'
                    },
                    multiple=True
                ), 
                    html.Div(id='output-data-upload')
                ]
            ),
         dcc.Tab( label = 'previev and edit', 
            children=[
                html.Div("Select dataframe:"),
                dcc.Dropdown(
                    id='df_selector3',
                    options=[{'label': i, 'value': i} for i in data_frames.keys()],
                    value='cases'),
                    
                html.Button('Refresh dataframes list', id='Ref_3', n_clicks=0),
                html.Div("Select colums to display:"),
                dcc.Dropdown(
                        id='col_selector_prev',
                        options=[{'label': i, 'value': i} for i in data_frames['cases'].columns],
                        placeholder="All",
                        multi=True),
                html.Div("Select colums to group by:"),
                dcc.Dropdown(
                        id='group_by_selector',
                        options=[{'label': i, 'value': i} for i in data_frames['cases'].columns],
                        placeholder="None",
                        multi=True),
                html.Div("Select group by function:"),
                dcc.RadioItems(
                        id='grp_by_how',
                        options=[{'label': i, 'value': i} for i in ['mean', 'sum']],
                        value = 'mean'
                ),
                
                html.Button('Apply changes to dataframe', id='Apply_changes_button', n_clicks=0),
                html.Button('Save as new dataframe', id='Save_new', n_clicks=0),
                dash_table.DataTable(
                    id='datatable-paging',
                    column_selectable="single",
                    page_current=0,
                    page_size=PAGE_SIZE,
                    page_action='custom',
                    style_table={'overflowX': 'scroll'}
                    )]
               ),
         dcc.Tab( label = 'join', 
                    children=[
                        html.Div("Select join type:"),
                        html.Div(dcc.RadioItems(
                                    id='merge_type',
                                    options=[{'label': i, 'value': i} for i in ['inner', 'left', 'right', 'outer', 'concatenate']],
                                    value = 'inner')
                                ),
                        html.Button('Refresh dataframes list', id='Ref_1_2', n_clicks=0),
                        html.Div(id ='content'),
                        html.Button('Merge', id='btn-nclicks-3', n_clicks=0)
                    ]
                 ),        
       
        dcc.Tab(label = 'graphs',
                children = [
                html.Div("Select plot type:"),
                dcc.RadioItems(
                        id='plot_type',
                        options=[{'label': i, 'value': i} for i in ['line', 'bar', 'scatter']],
                        value = 'line'
                ),
                html.Button('Refresh dataframes list', id='Ref_4', n_clicks=0),
                html.Div("Select data frame:"),
                dcc.Dropdown(
                    id='graph_df_dropdown',
                    options=[{'label': i, 'value': i} for i in data_frames.keys()]),
                html.Div(id = 'graphs_tab'),                 
            ])   
        ])
     ])
cl = {}
#merge callback
@app.callback(
    Output('container-button-timestamp', 'children'),
    Output('col_selector1', 'options'),
    Output('col_selector2', 'options'),
    Input('btn-nclicks-3', 'n_clicks'),
    Input('df_selector1', 'value'),
    Input('col_selector1', 'value'),
    Input('df_selector2', 'value'),
    Input('col_selector2', 'value'),
    Input('merge_type', 'value')
)
def merge_tab(btn3, df1, col1, df2, col2, merge_type):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    if 'btn-nclicks-3' in changed_id:
            if merge_type != 'concatenate':
                if col1 is not None and col2 is not None:
                    data_frames.update({"merged":merge(data_frames[df1], data_frames[df2], col1, col2, merge_type)})
                    msg = [html.Button("Download CSV", id="btn_csv"),
                          dcc.Download(id="download-dataframe-csv"),]
                else:
                    msg = html.H1("In order to performe mergr, please selct keys!"),
            else:
                data_frames.update({"merged":pd.concat([data_frames[df1], data_frames[df2]])})
                msg = [html.Button("Download CSV", id="btn_csv"),
                      dcc.Download(id="download-dataframe-csv"),]   
    else:
        msg = None
    if merge_type != 'concatenate':
        opt1 = [{'label': i, 'value': i} for i in data_frames[df1].columns]
        opt2 = [{'label': i, 'value': i} for i in data_frames[df2].columns]
    else:
        opt1 = [{'label': i, 'value': i} for i in ["N.A."]]
        opt2 = [{'label': i, 'value': i} for i in ["N.A."]]

    return msg, opt1, opt2

@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("btn_csv", "n_clicks"),
    prevent_initial_call=True,
)
def func(n_clicks):
    df = data_frames["merged"]
    return dcc.send_data_frame(df.to_csv, "merged_df.csv")

@app.callback(
    Output('content', 'children'),
    Input('merge_type', "value")
    )
def previrw_content(how):
    children = [    
        html.Div("Select left dataframe to merge:"),
        html.Div(dcc.Dropdown(
                            id='df_selector1',
                            options=[{'label': i, 'value': i} for i in data_frames.keys()],
                            value = 'cases')
                        ),
        html.Div("Select right dataframe to merge:"),
        html.Div(dcc.Dropdown(
                            id='df_selector2',
                            options=[{'label': i, 'value': i} for i in data_frames.keys()],
                            value = 'mobility')
                        )        
    ]

    children.append(html.Div("Select keys from left frame:"))
    children.append(html.Div(dcc.Dropdown(
                        id='col_selector1',
                        options=[{'label': i, 'value': i} for i in data_frames.keys()],
                        multi=True)
                    ))
    children.append(html.Div("Select keys from right frame:"))
    children.append(html.Div(dcc.Dropdown(
                        id='col_selector2',
                        options=[{'label': i, 'value': i} for i in data_frames.keys()],
                        multi=True)      
                            )
                   )

    children.append(html.Div(id='container-button-timestamp'))
                 
    return children  

                

        
# previev callback
@app.callback(
    Output('datatable-paging', 'data'),
    Output('datatable-paging', 'columns'),
    Output('col_selector_prev', "options"),
    Output('group_by_selector', "options"),
    Input('datatable-paging', "page_current"),
    Input('datatable-paging', "page_size"),
    Input('df_selector3', "value"),
    Input('col_selector_prev', "value"),
    Input('group_by_selector', "value"),
    Input('Apply_changes_button', "n_clicks"),
    Input('Save_new', "n_clicks"),
    Input('grp_by_how', "value")
    )
def update_table(page_current, page_size, dataframe, filtered_columns, group_by_columns, btn_apply, btn_save, grp_by_how):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    
    if 'Save_new' in changed_id:
        new_dataframe = "dataframe" + str(len(data_frames))
        if filtered_columns:
            data_frames[new_dataframe] = data_frames[dataframe][filtered_columns]
        else:
            data_frames[new_dataframe] = data_frames[dataframe]
        if group_by_columns:
            if grp_by_how == 'mean':
                data_frames[new_dataframe] = data_frames[dataframe].groupby(by=group_by_columns, as_index = False).mean()
            if grp_by_how == 'sum':
                data_frames[new_dataframe] = data_frames[dataframe].groupby(by=group_by_columns, as_index = False).sum()
        
    if 'Apply_changes_button' in changed_id:
        if filtered_columns:
            data_frames[dataframe] = data_frames[dataframe][filtered_columns]
        else:
            data_frames[dataframe] = data_frames[dataframe]
        if group_by_columns:
            if grp_by_how == 'mean':
                data_frames[dataframe] = data_frames[dataframe].groupby(by=group_by_columns, as_index = False).mean()
            if grp_by_how == 'sum':
                data_frames[dataframe] = data_frames[dataframe].groupby(by=group_by_columns, as_index = False).sum()
                
    df_filtered = data_frames[dataframe]
    if filtered_columns:
        df_filtered = df_filtered[filtered_columns]
    if group_by_columns:
        if grp_by_how == 'mean':
            df_filtered = df_filtered.groupby(by=group_by_columns, as_index = False).mean()
        if grp_by_how == 'sum':
            df_filtered = df_filtered.groupby(by=group_by_columns, as_index = False).sum()
    df_filtered = df_filtered[page_current*page_size:(page_current+ 1)*page_size]
    columns = [{"name": i, "id": i} for i in df_filtered.columns]
    opt = [{"label": i, "value": i} for i in data_frames[dataframe].columns]
    return df_filtered.to_dict('records'), columns, opt, opt

#upload callback
@app.callback(
    Output('output-data-upload', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
    State('upload-data', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        opt = [{'label': i, 'value': i} for i in data_frames.keys()]
        return children
    
#refresh_df_list
@app.callback(
        Output('df_selector1', 'options'),
        Output('df_selector2', 'options'),
        Output('df_selector3', 'options'),
        Output('graph_df_dropdown', 'options'),
        Input('Ref_1_2', 'n_clicks'),
        Input('Ref_3', 'n_clicks'),
        Input('Ref_4', 'n_clicks')
    )
def update_df_selectors(n1, n2, n4):
            opt = [{'label': i, 'value': i} for i in data_frames.keys()]
            return opt, opt, opt, opt

@app.callback(
    Output('graphs_tab', 'children'),
    Input('graph_df_dropdown', "value")
    )
def graph_content(df):
    children = []
    if df is not None:
        children.append(html.Div("Select data for x axis:"))
        children.append(html.Div(dcc.Dropdown(
                            id='graph_column_dropdown1',
                            options=[{'label': i, 'value': i} for i in data_frames[df].columns])
                        )
                       )
        children.append(html.Div("Select data for y axis:"))
        children.append(html.Div(dcc.Dropdown(
                            id='graph_column_dropdown2',
                            options=[{'label': i, 'value': i} for i in data_frames[df].columns])
                        )
                       )
        children.append(html.Div("Select data for labeling:"))
        children.append(html.Div(dcc.Dropdown(
                            id='graph_column_dropdown3',
                            options=[{'label': i, 'value': i} for i in data_frames[df].columns])
                        )
                       )
        children.append(html.Div(id = 'graph_container'))
                 
    return children  

@app.callback(
    Output('graph_container', 'children'),
    Input('graph_df_dropdown', "value"),
    Input('graph_column_dropdown1', "value"),
    Input('graph_column_dropdown2', "value"),
    Input('graph_column_dropdown3', "value"),
)
def graph_cont(df, x, y, z):
    if None not in (df, x, y):
        output = []
        if z is not None:
            output.append(html.Div('Filter by labels:'))
            output.append(dcc.Dropdown(
                            id='graph_value_dropdown',
                            options=[{'label': i, 'value': i} for i in data_frames[df][z].unique()],
                            multi = True,
                            placeholder="All"))
        else:
             output.append(dcc.Dropdown(
                            id='graph_value_dropdown',
                            options=[{'label': i, 'value': i} for i in data_frames[df][y].unique()],
                            multi = True,
                            value = None,
                            placeholder="All",
                            style={'display': 'none'}))
        output.append(html.Div(id = 'graph_container2'))    
        return output
    else:
        return None

@app.callback(
    Output('graph_container2', 'children'),
    Input('graph_df_dropdown', "value"),
    Input('graph_column_dropdown1', "value"),
    Input('graph_column_dropdown2', "value"),
    Input('graph_column_dropdown3', "value"),
    Input('graph_value_dropdown', "value"),
    Input('plot_type', "value") 
)
def graph_cont(df, x, y, z, flt, plot_type):   
    print(df, x, y, z, flt, plot_type)
    if flt is None:
        if plot_type == 'line':
            fig = dcc.Graph(figure = px.line(data_frames[df], x=data_frames[df][x], y=data_frames[df][y],color= z),
                        style={'width': '190vh', 'height': '65vh'})
        elif plot_type == 'bar': 
            fig = dcc.Graph(figure = px.bar(data_frames[df], x= x, y= y,color= z),
                            style={'width': '190vh', 'height': '65vh'})
        elif plot_type == 'scatter':    
            fig = dcc.Graph(figure = px.scatter(data_frames[df], x= x, y= y,color= z),
                            style={'width': '190vh', 'height': '65vh'})
        if None not in (df, x, y):
            return fig
        else:
            return None
    else:
        if plot_type == 'line':
            fig = dcc.Graph(figure = px.line(data_frames[df][data_frames[df][z].isin(flt)], x=  x, y= y,color= z),
                        style={'width': '190vh', 'height': '65vh'})
        elif plot_type == 'bar': 
            fig = dcc.Graph(figure = px.bar(data_frames[df][data_frames[df][z].isin(flt)], x= x, y= y,color= z),
                            style={'width': '190vh', 'height': '65vh'})
        elif plot_type == 'scatter':    
            fig = dcc.Graph(figure = px.scatter(data_frames[df][data_frames[df][z].isin(flt)], x= x, y= y,color= z),
                            style={'width': '190vh', 'height': '65vh'})
        if None not in (df, x, y):
            return fig
        else:
            return None


app.run_server(debug=False)


# In[ ]:




