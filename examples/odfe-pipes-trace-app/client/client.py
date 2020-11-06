# Client calling various operations in Sample App

from sys import argv
from requests import post
import mysql.connector
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
import time

OTLP = os.getenv("OTLP") if os.getenv("OTLP") is not None else "localhost"
INVENTORY = os.getenv("INVENTORY") if os.getenv("INVENTORY") is not None else "localhost"
PAYMENT = os.getenv("PAYMENT") if os.getenv("PAYMENT") is not None else "localhost"
AUTH = os.getenv("AUTH") if os.getenv("AUTH") is not None else "localhost"
SLEEP_TIME_IN_SECONDS = os.getenv("SLEEP_TIME_IN_SECONDS") if os.getenv("SLEEP_TIME_IN_SECONDS") is not None else 1

DB_NAME = 'APM'
HOST = os.getenv("MYSQL_HOST") if os.getenv("MYSQL_HOST") is not None else "localhost"
PORT = int(os.getenv("MYSQL_PORT")) if os.getenv("MYSQL_PORT") is not None else 3306

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
    INSERT_INITIAL_ROWS_CMD = """INSERT INTO Inventory_Items (ItemId, TotalQty) 
                           VALUES (%s, %s) """
    data=[
        ("apple", 4),
        ("orange", 10),
        ("banana", 6),
    ]

    cnx = getDBCnx()
    cursor = cnx.cursor()

    cursor.executemany(INSERT_INITIAL_ROWS_CMD, data)
    connection.commit()

    closeCursorAndDBCnx(cursor, cnx)

def cleanupDB():
    DELETE_INVENTORY = "DELETE FROM Inventory_Items;"

    cnx = getDBCnx()
    cursor = cnx.cursor()

    cursor.execute(DELETE_INVENTORY)
    connection.commit()

    closeCursorAndDBCnx(cursor, cnx)

def cartCheckout():
    # TODO: Cart Management Service will be used here once created.
    with tracer.start_as_current_span("client_cart_checkout"):
        checkoutAPIRequest = post(
            "http://{}:8084/checkout".format(PAYMENT),
            data=[
                ("banana", 2),
                ("orange", 3),
                ("apple", 1),
            ],
        )
        assert checkoutAPIRequest.status_code == 200

while True:
    setupDB()
    try:
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
        cartCheckout()
        time.sleep(5)
    except:
        cleanupDB()
        continue
