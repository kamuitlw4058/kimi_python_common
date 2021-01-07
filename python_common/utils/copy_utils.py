import os
import dash
from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc

app = dash.Dash(__name__)
base_dir = 'data/copy_util'

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

file_path = f'{base_dir}/content.txt'

app.layout = html.Div([
    html.H1(children='Clip Helper'),
    dcc.Textarea(
        id='textarea-example',
        value='Textarea content initialized\nwith multiple lines of text',
        style={'width': '100%', 'height': 300},
    ),
    html.Div(id='textarea-example-output', style={'whiteSpace': 'pre-line'})
])


@app.callback(
    Output('textarea-example-output', 'children'),
    Input('textarea-example', 'value')
)
def update_output(value):
    with open(file_path,'w') as f:
        f.write(value)

    return 'You have entered: \n{}'.format(value)

if __name__ == '__main__':
    app.run_server(port=11111,host='0.0.0.0')