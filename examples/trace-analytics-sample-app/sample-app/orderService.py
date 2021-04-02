# OrderService running on port 8088

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
from requests import delete, get, post, put
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
                "service.name": "order",
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

@app.route("/update_order", methods=["POST", "PUT"])
def update_order():
    errorRate = random.randint(0,99)
    if errorRate < UPDATE_ERROR_RATE_THRESHOLD:
        logs('Order', 'Update operation failed - Service Unavailable: 503')
        raise Error('Update Order Failed - Service Unavailable', status_code=503)
    else:
        with tracer.start_as_current_span("update_order"):
            rawData = request.form
            failedItems = []
            for itemId in rawData.keys():
                qty = sum([val for val in rawData.getlist(itemId, type=int)])

                if qty >= 0:
                    databaseResponse = post(
                        "http://{}:8083/add_item_to_cart".format(DATABASE),
                        data={"ItemId": itemId, "Qty": qty})
                else:
                    databaseResponse = post(
                        "http://{}:8083/remove_item_from_cart".format(DATABASE),
                        data={"ItemId": itemId, "Qty": -qty})

                if databaseResponse.status_code != 200:
                    failedItems.append(itemId)

            if len(failedItems)>0:
                response = jsonify(
                    {"failed_items": failedItems}
                )
                response.status_code = 206
                response.status = "{} {}".format(
                    response.status_code, "Update order response contains failed items."
                )
                return response
            else:
                logs('Order', 'Update operations successful')
                return jsonify({"failed_items": []})

@app.route("/get_order")
def get_order():
    errorRate = random.randint(0,99)
    if errorRate < READ_ERROR_RATE_THRESHOLD:
        logs('Order', 'Read operation failed - Service Unavailable: 503')
        raise Error('getOrder Failed - Service Unavailable', status_code=503)
    else:
        with tracer.start_as_current_span("get_order"):
            databaseResponse = get(
                "http://{}:8083/get_cart".format(DATABASE))
            assert databaseResponse.status_code == 200
            logs('Order', 'Read operation successful')
            return databaseResponse.json()

@app.route("/clear_order", methods=["DELETE"])
def clear_order():
    errorRate = random.randint(0,99)
    if errorRate < DELETE_ERROR_RATE_THRESHOLD:
        logs('Order', 'Update operation failed - Service Unavailable: 503')
        raise Error('clearOrder Failed - Service Unavailable', status_code=503)
    else:
        with tracer.start_as_current_span("clear_order"):
            databaseResponse = put(
                "http://{}:8083/cart_empty".format(DATABASE),
            )
            assert databaseResponse.status_code == 200

            logs('Order', 'Delete operation successful')
            return "success"

@app.route("/pay_order", methods=["POST", "GET"])
def pay_order():
    with tracer.start_as_current_span("pay_order"):
        databaseResponse = delete(
            "http://{}:8083/cart_sold".format(DATABASE),
        )
        assert databaseResponse.status_code == 200

        logs('Order', 'Update operation successful')
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
    app.run(port=8088, host="0.0.0.0")