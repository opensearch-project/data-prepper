# DatabaseService running on port 8083.

import mysql.connector
from flask import Flask, jsonify, request
from mysql.connector import errorcode
from opentelemetry import trace
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.mysql import MySQLInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.wsgi import collect_request_attributes
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
import opentelemetry.instrumentation.requests
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
import flask, os, pkg_resources
import socket

app = Flask(__name__)

OTLP = os.getenv("OTLP") if os.getenv("OTLP") is not None else "localhost"

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create(
            {
                "service.name": "database",
                "service.instance.id": str(id(app)),
                "telemetry.sdk.name": "opentelemetry",
                "telemetry.sdk.language": "python",
                "telemetry.sdk.version": pkg_resources.get_distribution("opentelemetry-sdk").version,
                "host.hostname": socket.gethostname(),
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

DB_NAME = 'APM'
HOST = os.getenv("MYSQL_HOST") if os.getenv("MYSQL_HOST") is not None else "localhost"
PORT = int(os.getenv("MYSQL_PORT")) if os.getenv("MYSQL_PORT") is not None else 3306

TABLES = {}
TABLES['Inventory_Items'] = (
    "CREATE TABLE `Inventory_Items` ("
    "  `ItemId` varchar(16) NOT NULL,"
    "  `TotalQty` int(11) NOT NULL,"
    "  PRIMARY KEY (`ItemId`)"
    ") ENGINE=InnoDB")

TABLES['User_Carts'] = (
    "CREATE TABLE `User_Carts` ("
    "  `ItemId` varchar(16) NOT NULL,"
    "  `TotalQty` int(11) NOT NULL,"
    "  PRIMARY KEY (`ItemId`)"
    ") ENGINE=InnoDB"
)

def getCnx():
    cnx = mysql.connector.connect(user="root", host=HOST, port=PORT)
    return cnx

def getDBCnx():
    cnx = mysql.connector.connect(user="root", host=HOST, port=PORT, database=DB_NAME)
    return cnx

def closeCursorAndDBCnx(cursor, cnx):
    cursor.close()
    cnx.close()

def createDatabase(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def createOrUseDatabase(cnx):
    try:
        cnx.cursor().execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exist.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            createDatabase(cnx.cursor())
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)

def createTables(cursor):
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

MySQLInstrumentor().instrument()
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument(tracer_provider=tracerProvider)

@app.route("/database")
def database():
    with tracer.start_as_current_span("database"):
        return "database"

UPDATE_INVENTORY_ITEM_CMD = ("INSERT INTO Inventory_Items (ItemId, TotalQty) VALUES "
                    "(%(ItemId)s, %(Qty)s) ON DUPLICATE KEY UPDATE TotalQty = TotalQty + %(Qty)s")

@app.route("/update_item", methods=["POST", "PUT"])
def updateItem():
    with tracer.start_as_current_span("update_item"):
        data = request.form.to_dict()
        
        qty = int(data.get("Qty"))

        cnx = getDBCnx()
        cursor = cnx.cursor()

        if qty >= 0:
            cursor.execute(UPDATE_INVENTORY_ITEM_CMD, data)
            cnx.commit()

            closeCursorAndDBCnx(cursor, cnx)

            return "success"
        else:
            data["Qty"] = -qty
            cursor.execute(CONDITIONAL_RMV_ITEM_FROM_INVENTORY_CMD, data)

            if cursor.rowcount > 0:
                cnx.commit()
                
                closeCursorAndDBCnx(cursor, cnx)

                return "success"
            else:
                cnx.rollback()
                
                closeCursorAndDBCnx(cursor, cnx)

                raise InvalidItemUpdate("Not enough storage for itemId {}".format(data["ItemId"]))

class InvalidItemUpdate(Exception):
    status_code = 422

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@app.errorhandler(InvalidItemUpdate)
def handle_invalid_item_update(error):
    response = jsonify(error.to_dict())
    response.status = "{} {}".format(error.status_code, error.message)
    return response

GET_ITEMS_CMD = "SELECT ItemId FROM Inventory_Items"

@app.route("/get_items")
def getItems():
    with tracer.start_as_current_span("get_items"):
        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute(GET_ITEMS_CMD)

        rows = []
        for (itemId,) in cursor:
            rows.append(itemId)

        closeCursorAndDBCnx(cursor, cnx)

        return ",".join(rows)

GET_INVENTORY_CMD = "SELECT * FROM Inventory_Items"

@app.route("/get_inventory")
def getIntentory():
    with tracer.start_as_current_span("get_inventory"):
        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute(GET_INVENTORY_CMD)

        data = {}
        for (itemId, totalQty) in cursor:
            data[itemId] = int(totalQty)

        closeCursorAndDBCnx(cursor, cnx)

        return data

GET_CART_CMD = "SELECT * FROM User_Carts"

@app.route("/get_cart")
def getCart():
    with tracer.start_as_current_span("get_cart"):
        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute(GET_CART_CMD)

        data = {}
        for (itemId, totalQty) in cursor:
            data[itemId] = int(totalQty)

        closeCursorAndDBCnx(cursor, cnx)

        return data

CONDITIONAL_RMV_ITEM_FROM_INVENTORY_CMD = ("UPDATE Inventory_Items "
                            "SET TotalQty=IF(TotalQty >= %(Qty)s, TotalQty - %(Qty)s, TotalQty) "
                            "WHERE ItemId=%(ItemId)s")
UPDATE_USER_CART_ITEM_CMD = (
    "INSERT INTO User_Carts (ItemId, TotalQty) VALUES "
    "(%(ItemId)s, %(Qty)s) ON DUPLICATE KEY UPDATE TotalQty = TotalQty + %(Qty)s"
)

@app.route("/add_item_to_cart", methods=["POST", "PUT"])
def addItemToCart():
    with tracer.start_as_current_span("add_item_to_cart"):
        # (ItemId, Qty)
        data = request.form.to_dict()

        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute(CONDITIONAL_RMV_ITEM_FROM_INVENTORY_CMD, data)
        if cursor.rowcount > 0:
            cursor.execute(UPDATE_USER_CART_ITEM_CMD, data)
            cnx.commit()

            closeCursorAndDBCnx(cursor, cnx)

            return "success"
        else:
            cnx.rollback()

            closeCursorAndDBCnx(cursor, cnx)

            return "fail"

CONDITIONAL_RMV_ITEM_FROM_CART_CMD = ("UPDATE User_Carts "
                            "SET TotalQty=IF(TotalQty >= %(Qty)s, TotalQty - %(Qty)s, TotalQty) "
                            "WHERE ItemId=%(ItemId)s")

@app.route("/remove_item_from_cart", methods=["POST", "PUT"])
def removeItemFromCart():
    with tracer.start_as_current_span("remove_item_from_cart"):
        # (ItemId, Qty)
        data = request.form.to_dict()
        
        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute(CONDITIONAL_RMV_ITEM_FROM_CART_CMD, data)
        if cursor.rowcount > 0:
            cursor.execute(UPDATE_INVENTORY_ITEM_CMD, data)
            cnx.commit()

            closeCursorAndDBCnx(cursor, cnx)

            return "success"
        else:
            cnx.rollback()

            closeCursorAndDBCnx(cursor, cnx)

            return "fail"

@app.route("/cart_sold", methods=["DELETE"])
def cartSold():
    with tracer.start_as_current_span("cart_sold"):
        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute("TRUNCATE TABLE User_Carts")

        closeCursorAndDBCnx(cursor, cnx)

        return "success"

@app.route("/cart_empty", methods=["PUT"])
def cartEmpty():
    with tracer.start_as_current_span("cart_empty"):
        cnx = getDBCnx()
        cursor = cnx.cursor()

        cursor.execute("SELECT ItemId, TotalQty FROM User_Carts")
        for (itemId, qty) in cursor.fetchall():
            data={"ItemId": itemId, "Qty": qty}
            cursor.execute(UPDATE_INVENTORY_ITEM_CMD, data)
            # Prevent UnreadResult Error
            cursor.nextset()
        cnx.commit()
        cursor.execute("TRUNCATE TABLE User_Carts")

        closeCursorAndDBCnx(cursor, cnx)

        return "success"

if __name__ == "__main__":
    cnx = getCnx()
    createOrUseDatabase(cnx)
    cursor = cnx.cursor()
    createTables(cursor)
    closeCursorAndDBCnx(cursor, cnx)
    app.run(port=8083, host="0.0.0.0")