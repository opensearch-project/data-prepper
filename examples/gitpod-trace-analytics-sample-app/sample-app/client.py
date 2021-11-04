# Client calling various operations in Sample App

from sys import argv
from requests import delete, get, post
import mysql.connector
import dash
import dash_html_components as html
from dash.dependencies import Input, Output, State
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from urllib3.util.retry import Retry
import os, pkg_resources, socket, requests

OTLP = os.getenv("OTLP") if os.getenv("OTLP") is not None else "localhost"
ORDER = os.getenv("ORDER") if os.getenv("ORDER") is not None else "localhost"
INVENTORY = os.getenv("INVENTORY") if os.getenv("INVENTORY") is not None else "localhost"
PAYMENT = os.getenv("PAYMENT") if os.getenv("PAYMENT") is not None else "localhost"
AUTH = os.getenv("AUTH") if os.getenv("AUTH") is not None else "localhost"
SLEEP_TIME_IN_SECONDS = os.getenv("SLEEP_TIME_IN_SECONDS") if os.getenv("SLEEP_TIME_IN_SECONDS") is not None else 1

DB_NAME = 'APM'
HOST = os.getenv("MYSQL_HOST") if os.getenv("MYSQL_HOST") is not None else "localhost"
PORT = int(os.getenv("MYSQL_PORT")) if os.getenv("MYSQL_PORT") is not None else 3306

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'Data Prepper Sample App'

app.layout = html.Div(id="main", children=[
    html.H1("Trace Analytics Sample App", style={'textAlign': 'left'}),
    html.Div(style={'padding': 25}),
    html.Div([
            html.A("Trace Analytics Dashboard", href="http://localhost:5601/app/trace-analytics-dashboards#/",
                   style={'justify': 'center'})
        ], style={'horizontalAlign': 'middle','verticalAlign': 'middle'}),
    html.Div(style={'padding': 10}),
    html.Div([
            html.A("Trace Analytics Services View", href="http://localhost:5601/app/trace-analytics-dashboards#/services",
                   style={'justify': 'center'})
        ], style={'horizontalAlign': 'middle','verticalAlign': 'middle'}),
    html.Div(style={'padding': 25}),
    html.Div([html.Button('Checkout', id='btn-nclicks-1', n_clicks=0, style={'background-color': '#729bb7'})],
             style={'horizontalAlign': 'middle','verticalAlign': 'middle'}),
    html.Div(style={'padding': 10}),
    html.Div([html.Button('Cancel', id='btn-nclicks-2', n_clicks=0, style={'background-color': '#ACC58C'})],
             style={'horizontalAlign': 'middle', 'verticalAlign': 'middle'}),
    html.Div(style={'padding': 10}),
    html.Div([html.Button('Create', id='btn-nclicks-3', n_clicks=0, style={'background-color': '#BE93D0'})],
             style={'horizontalAlign': 'middle', 'verticalAlign': 'middle'}),
    html.Div(style={'padding': 10}),
    html.Div([html.Button('Status', id='btn-nclicks-4', n_clicks=0, style={'background-color': '#CED093'})],
             style={'horizontalAlign': 'middle', 'verticalAlign': 'middle'}),
    html.Div(style={'padding': 10}),
    html.Div([html.Button('Pay', id='btn-nclicks-5', n_clicks=0, style={'background-color': '#D09396'})],
             style={'horizontalAlign': 'middle', 'verticalAlign': 'middle'}),
    html.Div(style={'padding': 50}),
    html.Div(id="output", children=[])
])


@app.callback(Output('output', 'children'),
              [Input('btn-nclicks-1', 'n_clicks'),
              Input('btn-nclicks-2', 'n_clicks'),
              Input('btn-nclicks-3', 'n_clicks'),
              Input('btn-nclicks-4', 'n_clicks'),
              Input('btn-nclicks-5', 'n_clicks')],
              [State('output', 'children')])
def displayClick(btn1, btn2, btn3, btn4, btn5, old_output):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    if 'btn-nclicks-1' in changed_id:
        return checkout() + old_output
    elif 'btn-nclicks-2' in changed_id:
        return cancelOrder() + old_output
    elif 'btn-nclicks-3' in changed_id:
        return createOrder() + old_output
    elif 'btn-nclicks-4' in changed_id:
        return deliveryStatus() + old_output
    elif 'btn-nclicks-5' in changed_id:
        return payOrder() + old_output
    else:
        load_main_screen()
        return old_output


trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create(
            {
                "service.name": "frontend-client",
                "host.hostname": socket.gethostname(),
                "telemetry.sdk.name": "opentelemetry",
                "telemetry.sdk.language": "python",
                "telemetry.sdk.version": pkg_resources.get_distribution("opentelemetry-sdk").version,
            }
        )
    )
)
tracerProvider = trace.get_tracer_provider()
tracer = tracerProvider.get_tracer(__name__)

tracerProvider.add_span_processor(
    SimpleSpanProcessor(ConsoleSpanExporter())
)
otlp_exporter = OTLPSpanExporter(endpoint="{}:55680".format(OTLP), insecure=True)
tracerProvider.add_span_processor(
    SimpleSpanProcessor(otlp_exporter)
)
RequestsInstrumentor().instrument(tracer_provider=tracerProvider)

retry_strategy = Retry(
    total=2,
    status_forcelist=[401, 401.1, 429, 503],
    method_whitelist=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
)


def getDBCnx():
    cnx = mysql.connector.connect(user="root", host=HOST, port=PORT, database=DB_NAME)
    return cnx


def closeCursorAndDBCnx(cursor, cnx):
    cursor.close()
    cnx.close()


def setupDB():
    INSERT_ROWS_CMD = """INSERT INTO Inventory_Items (ItemId, TotalQty) 
                           VALUES (%(ItemId)s, %(Qty)s) ON DUPLICATE KEY UPDATE TotalQty = TotalQty + %(Qty)s"""
    data = [
        {"ItemId": "apple", "Qty": 4},
        {"ItemId": "orange", "Qty": 10},
        {"ItemId": "banana", "Qty": 6},
    ]

    cnx = getDBCnx()
    cursor = cnx.cursor()

    for item in data:
        cursor.execute(INSERT_ROWS_CMD, item)
        cnx.commit()

    closeCursorAndDBCnx(cursor, cnx)


def cleanupDB():
    DELETE_INVENTORY = "DELETE FROM Inventory_Items;"

    cnx = getDBCnx()
    cursor = cnx.cursor()

    cursor.execute(DELETE_INVENTORY)
    cnx.commit()

    closeCursorAndDBCnx(cursor, cnx)


def checkout():
    trace_id = None
    try:
        with tracer.start_as_current_span("client_checkout") as checkout_trace_group:
            trace_id = get_hexadecimal_trace_id(checkout_trace_group.get_span_context().trace_id)
            checkoutAPIRequest = post(
                "http://{}:8084/checkout".format(PAYMENT),
                data=[
                    ("banana", 2),
                    ("orange", 3),
                    ("apple", 1),
                ],
            )
            assert checkoutAPIRequest.status_code == 200
            return get_ref_link("Checkout", "success", trace_id)
    except:
        return get_ref_link("Checkout", "failed", trace_id)


def createOrder():
    trace_id = None
    try:
        with tracer.start_as_current_span("client_create_order") as create_order_trace_group:
            trace_id = get_hexadecimal_trace_id(create_order_trace_group.get_span_context().trace_id)
            updateOrderAPIRequest = post(
                "http://{}:8088/update_order".format(ORDER),
                data=[
                    ("apple", 1),
                    ("orange", 3),
                    ("banana", 2)
                ]
            )
            assert updateOrderAPIRequest.status_code == 200
            return get_ref_link("Create", "success", trace_id)
    except:
        return get_ref_link("Create", "failed", trace_id)

def cancelOrder():
    trace_id = None
    try:
        with tracer.start_as_current_span("client_cancel_order") as cancel_order_trace_group:
            trace_id=get_hexadecimal_trace_id(cancel_order_trace_group.get_span_context().trace_id)
            cancelOrderAPIRequest = delete("http://{}:8088/clear_order".format(ORDER))
            assert cancelOrderAPIRequest.status_code == 200
            return get_ref_link("Cancel", "success", trace_id)
    except:
        return get_ref_link("Cancel", "failed", trace_id)


def deliveryStatus():
    trace_id = None
    try:
        with tracer.start_as_current_span("client_delivery_status") as delivery_status_trace_group:
            trace_id = get_hexadecimal_trace_id(delivery_status_trace_group.get_span_context().trace_id)
            getOrderAPIRequest = get("http://{}:8088/get_order".format(ORDER))
            assert getOrderAPIRequest.status_code == 200
            return get_ref_link("Status", "success", trace_id)
    except:
        return get_ref_link("Status", "failed", trace_id)


def payOrder():
    trace_id = None
    try:
        with tracer.start_as_current_span("client_pay_order") as pay_order_trace_group:
            trace_id = get_hexadecimal_trace_id(pay_order_trace_group.get_span_context().trace_id)
            payOrderAPIRequest = post("http://{}:8088/pay_order".format(ORDER))
            assert payOrderAPIRequest.status_code == 200
            return get_ref_link("Pay", "success", trace_id)
    except:
        return get_ref_link("Pay", "failed", trace_id)

def load_main_screen():
    setupDB()
    # Client attempts login with authentication.
    with tracer.start_as_current_span("load_main_screen"):
        # No retry because if error occurs we want to throw it to Kibana.
        loginSession = requests.Session()
        loginAPIResponse = loginSession.get(
            "http://{}:8085/server_request_login".format(AUTH)
        )
        loginSession.close()
        if loginAPIResponse.status_code != 200:
            loginAPIResponse.raise_for_status()


def get_ref_link(operation, status, trace_id):
    return [html.Div([html.A("{} {}. {}".format(operation, status, trace_id),
                  href="http://localhost:5601/app/trace-analytics-dashboards#/traces/{}".format(trace_id))])]

def get_hexadecimal_trace_id(trace_id: int) -> str:
    return bytes(bytearray.fromhex("{:032x}".format(trace_id))).hex()


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=8089)
