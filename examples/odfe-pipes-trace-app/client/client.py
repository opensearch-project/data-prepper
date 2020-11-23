# Client calling various operations in Sample App

from sys import argv
from requests import delete, get, post
import mysql.connector
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from opentelemetry import trace
from opentelemetry.exporter.otlp.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleExportSpanProcessor,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from requests.adapters import HTTPAdapter
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

app.layout = html.Div([
    html.H6("Trace App UI"),
    dcc.Dropdown(
        id="dropdown",
        options=[
            {'label': 'checkout', 'value': 'checkout'},
            {'label': 'create_order', 'value': 'createOrder'},
            {'label': 'cancel_order', 'value': 'cancelOrder'},
            {'label': 'delivery_status', 'value': 'deliveryStatus'},
            {'label': 'pay_order', 'value': 'payOrder'}
        ],
        placeholder="Select an option"
    ),
    html.Div(id='dropdown-output')
])

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
    SimpleExportSpanProcessor(ConsoleSpanExporter())
)
otlp_exporter = OTLPSpanExporter(endpoint="{}:55680".format(OTLP))
tracerProvider.add_span_processor(
    SimpleExportSpanProcessor(otlp_exporter)
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
    with tracer.start_as_current_span("client_checkout"):
        checkoutAPIRequest = post(
            "http://{}:8084/checkout".format(PAYMENT),
            data=[
                ("banana", 2),
                ("orange", 3),
                ("apple", 1),
            ],
        )
        assert checkoutAPIRequest.status_code == 200


def createOrder():
    with tracer.start_as_current_span("client_create_order"):
        updateOrderAPIRequest = post(
            "http://{}:8088/update_order".format(ORDER),
            data=[
                ("apple", 1),
                ("orange", 3),
                ("banana", 2)
            ]
        )
        assert updateOrderAPIRequest.status_code == 200


def cancelOrder():
    with tracer.start_as_current_span("client_cancel_order"):
        cancelOrderAPIRequest = delete("http://{}:8088/clear_order".format(ORDER))
        assert cancelOrderAPIRequest.status_code == 200


def deliveryStatus():
    with tracer.start_as_current_span("client_delivery_status"):
        getOrderAPIRequest = get("http://{}:8088/get_order".format(ORDER))
        assert getOrderAPIRequest.status_code == 200


def payOrder():
    with tracer.start_as_current_span("client_pay_order"):
        payOrderAPIRequest = post("http://{}:8088/pay_order".format(ORDER))
        assert payOrderAPIRequest.status_code == 200


@app.callback(Output('dropdown-output', 'children'),
              [Input('dropdown', 'value')])
def update_output(value):
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
    if value == 'checkout':
        return checkout()
    elif value == 'createOrder':
        return createOrder()
    elif value == 'cancelOrder':
        return cancelOrder()
    elif value == 'deliveryStatus':
        return deliveryStatus()
    elif value == 'payOrder':
        return payOrder()
    else:
        return 'No option selected'


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", port=8089)
