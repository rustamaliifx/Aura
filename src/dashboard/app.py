import dash 
from dash import Dash, dcc, Input, Output, html 
import plotly.express as px 
import plotly.graph_objs as go
import pandas as pd 
import dash_bootstrap_components as dbc
import requests
from datetime import datetime, timedelta
import numpy as np
import warnings

# Suppress datetime parsing warnings
warnings.filterwarnings("ignore", message="Could not infer format")

def safe_datetime_conversion(series):
    """Safely convert series to datetime with explicit format handling."""
    try:
        # Try common datetime formats first
        formats_to_try = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO format with microseconds
            '%Y-%m-%dT%H:%M:%SZ',     # ISO format without microseconds
            '%Y-%m-%dT%H:%M:%S',      # ISO format without timezone
            '%Y-%m-%d %H:%M:%S',      # Standard format
        ]
        
        for fmt in formats_to_try:
            try:
                return pd.to_datetime(series, format=fmt)
            except:
                continue
        
        # If none work, use dateutil parser but suppress warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pd.to_datetime(series, infer_datetime_format=True)
    except:
        # If all fails, return the original series
        return series 

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([ 
    dbc.Row([
        dbc.Col(html.H1("Anomaly Detector"), className="text-center my-4")
    ]),

    dbc.Row([ 
        dbc.Col([  
            dbc.Card([ 
                dbc.CardBody([ 
                    html.H4("Dashboard", className="text-center"),
                    dcc.Graph(id='anomaly-graph')
                ])
            ])
        ])
    ]),

    dbc.Row([
        dbc.Col([ 
            dbc.Card([ 
                dbc.CardBody([ 
                    html.H4("Distribution of Anomalies according to months", className="text-center"),
                    dcc.Graph(id='month-graph')
                ])
            ])
        ]),
        dbc.Col([ 
            dbc.Card([ 
                dbc.CardBody([ 
                    html.H4("Distribution of Anomalies according to weeks", className='text-center'),
                    dcc.Graph(id='week-graph')
                ])
            ])
        ])
    ]),
    
    # Add interval component for real-time updates
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Update every 5 seconds
        n_intervals=0
    ),


], fluid=True)

# Callback for real-time anomaly graph
@app.callback(
    Output('anomaly-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_anomaly_graph(n):
    try:
        # Fetch all data from FastAPI (including normal and anomalous points)
        print(f"Fetching data from API... (attempt #{n})")
        response = requests.get('http://localhost:8000/api/data', timeout=10)
        print(f"API response status: {response.status_code}")
        
        data = response.json()
        print(f"API response keys: {data.keys()}")
        
        all_data = data['data']
        print(f"Number of data points received: {len(all_data)}")

        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {list(df.columns)}")
        print("First few rows:")
        print(df.head())
        
        # Create time series plot
        fig = go.Figure()
        
        # Check if we have timestamp and value columns
        timestamp_col = None
        value_col = None
        
        # Look for timestamp column (could be @timestamp, timestamp, etc.)
        for col in df.columns:
            if 'timestamp' in col.lower():
                timestamp_col = col
                break
        
        print(f"Found timestamp column: {timestamp_col}")
        
        # Look for a numeric value column to plot
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            value_col = numeric_cols[0]  # Use first numeric column
        value_col = numeric_cols[15]
            
        print(f"Found numeric columns: {numeric_cols}")
        print(f"Selected value column: {value_col}")
        
        if timestamp_col and value_col:
            # Separate normal and anomalous data
            normal_data = df[df['is_anomaly'] == 1]
            anomaly_data = df[df['is_anomaly'] == -1]
            
            print(f"Normal data points: {len(normal_data)}")
            print(f"Anomalous data points: {len(anomaly_data)}")
            
            # Plot normal data points
            if not normal_data.empty:
                fig.add_trace(go.Scatter(
                    x=normal_data[timestamp_col],
                    y=normal_data[value_col],
                    mode='lines+markers',
                    name='Normal Data',
                    line=dict(color='blue', width=1),
                    marker=dict(size=4, color='blue'),
                    opacity=0.7
                ))
            
            # Highlight anomalies
            if not anomaly_data.empty:
                fig.add_trace(go.Scatter(
                    x=anomaly_data[timestamp_col],
                    y=anomaly_data[value_col],
                    mode='markers',
                    marker=dict(color='red', size=8, symbol='x'),
                    name='Anomalies',
                    opacity=1.0
                ))
            
            fig.update_layout(
                title=f'Real-time Anomaly Detection - {data["total_count"]} total points ({data["anomaly_count"]} anomalies)',
                xaxis={'title': 'Timestamp'},
                yaxis={'title': value_col},
                template='plotly_white',
                showlegend=True,
                hovermode='closest'
            )
        else:
            print("No suitable timestamp/value columns found, using fallback visualization")
            # Fallback: create a simple scatter plot if no timestamp/value columns found
            anomaly_colors = ['red' if row.get('is_anomaly', False) else 'blue' for _, row in df.iterrows()]
            print(f"Anomaly color distribution: Red={anomaly_colors.count('red')}, Blue={anomaly_colors.count('blue')}")
            
            fig.add_trace(go.Scatter(
                x=list(range(len(df))),
                y=[1] * len(df),
                mode='markers',
                marker=dict(
                    color=anomaly_colors,
                    size=8
                ),
                name='Data Points'
            ))
            
            fig.update_layout(
                title=f'Data Overview - {data["total_count"]} total points ({data["anomaly_count"]} anomalies)',
                xaxis={'title': 'Index'},
                yaxis={'title': 'Data Points'},
                template='plotly_white'
            )
        
        return fig
        
    except requests.exceptions.RequestException as e:
        print(f"API connection error: {e}")
        return {
            'data': [],
            'layout': go.Layout(
                title=f'API Connection Error: {str(e)}. Make sure FastAPI is running on port 8000.',
                template='plotly_white'
            )
        }
    except Exception as e:
        print(f"General error: {e}")
        return {
            'data': [],
            'layout': go.Layout(
                title=f'Error fetching data: {str(e)}',
                template='plotly_white'
            )
        }


if __name__ == '__main__':
    app.run(debug=True, port=8050) 
