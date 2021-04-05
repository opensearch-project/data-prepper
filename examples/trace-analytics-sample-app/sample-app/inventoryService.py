# InventoryService running on port 8082

from flask import Flask, jsonify, request, Response
from opentelemetry import trace
from opentelemetry.instrumentation.wsgi import collect_request_attributes
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from Error import Error
from requests import get, post
import flask, os, pkg_resources, socket, random, requests, json

READ_ERROR_RATE_THRESHOLD = 10
UPDATE_ERROR_RATE_THRESHOLD = 10
DELETE_ERROR_RATE_THRESHOLD = 10

app = Flask(__name__)

OTLP = os.getenv("OTLP") if os.getenv("OTLP") is not None else "localhost"
DATABASE = os.getenv("DATABASE") if os.getenv("DATABASE") is not None else "localhost"
LOGS = os.getenv("LOGS") if os.getenv("LOGS") is not None else "localhost"

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create(
            {
                "service.name": "inventory",
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

FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument(tracer_provider=tracerProvider)

@app.errorhandler(Error)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route("/read_inventory")
def read_inventory():
    errorRate = random.randint(0,99)
    if errorRate < READ_ERROR_RATE_THRESHOLD:
        logs('Inventory', 'Read operation failed - Service Unavailable: 503')
        raise Error('Read Inventory Failed - Service Unavailable', status_code=503)
    else:
        with tracer.start_as_current_span("read_inventory"):
            databaseResponse = get(
                "http://{}:8083/get_inventory".format(DATABASE))
            assert databaseResponse.status_code == 200
            logs('Inventory', 'Read operation successful')
            return databaseResponse.json()

@app.route("/update_inventory", methods=["POST", "PUT"])
def update_inventory():
    errorRate = random.randint(0,99)
    if errorRate < UPDATE_ERROR_RATE_THRESHOLD:
        logs('Inventory', 'Update operation failed - Service Unavailable: 503')
        raise Error('Update Inventory Failed - Service Unavailable', status_code=503)
    else:
        with tracer.start_as_current_span("update_inventory"):
            rawData = request.form
            failedItems = []
            for itemId in rawData.keys():
                qty = sum([val for val in rawData.getlist(itemId, type=int)])

                databaseResponse = post(
                    "http://{}:8083/update_item".format(DATABASE),
                    data={"ItemId": itemId, "Qty": qty})

                if databaseResponse.status_code != 200:
                    failedItems.append(itemId)

            if len(failedItems)>0:
                response = jsonify(
                    {"failed_items": failedItems}
                )
                response.status_code = 206
                response.status = "{} {}".format(
                    response.status_code, "Update inventory response contains failed items."
                )
                return response
            else:
                logs('Inventory', 'Update operations successful')
                return jsonify({"failed_items": []})

@app.route("/delete_inventory", methods=["DELETE"])
def delete_inventory():
    errorRate = random.randint(0,99)
    if errorRate < DELETE_ERROR_RATE_THRESHOLD:
        logs('Inventory', 'Update operation failed - Service Unavailable: 503')
        raise Error('Delete Inventory Failed - Service Unavailable', status_code=503)
    else:
        with tracer.start_as_current_span("delete_inventory"):
            databaseResponse = get(
                "http://{}:8083/get_inventory".format(DATABASE),
            )
            assert databaseResponse.status_code == 200

            for itemId, qty in databaseResponse.json().items():
                updateItemResponse = post(
                    "http://{}:8083/update_item".format(DATABASE),
                    data={"ItemId": itemId, "Qty": -int(qty)},
                )
                assert updateItemResponse.status_code == 200

            logs('Inventory', 'Delete operation successful')
            return "success"

def logs(serv=None, mes=None):
    create_log_data = {'service': serv, 'message': mes}
    url = "http://{}:8087/logs".format(LOGS)
    response = requests.post(
        url, data=json.dumps(create_log_data),
        headers={'Content-Type': 'application/json'}
    )
    assert response.status_code == 200
    return "success"

if __name__ == "__main__":
    app.run(port=8082, host="0.0.0.0")