# RecommendationService running on port 8086

from flask import Flask, jsonify, request
from opentelemetry import trace
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os, pkg_resources, socket, random, requests, json

RECOMMEND_ERROR_RATE_THRESHOLD = 10

app = Flask(__name__)

OTLP = os.getenv("OTLP") if os.getenv("OTLP") is not None else "localhost"
INVENTORY = os.getenv("INVENTORY") if os.getenv("INVENTORY") is not None else "localhost"
LOGS = os.getenv("LOGS") if os.getenv("LOGS") is not None else "localhost"

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create(
            {
                "service.name": "recommendation",
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

retry_strategy = Retry(
    total=2,
    status_forcelist=[401, 401.1, 429, 503],
    method_whitelist=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
)

@app.errorhandler(Error)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route("/recommend")
def recommend():
    errorRate = random.randint(0,99)
    if errorRate < RECOMMEND_ERROR_RATE_THRESHOLD:
        logs('Recommendation', 'Service is overwhelmed with TooManyRequests: 429')
        raise Error('Recommendation Retrieval Failed; TooManyRequests', status_code=429)
    else:
        with tracer.start_as_current_span("recommend"):
            num = request.args.get("num", type=int)

            readSession = requests.Session()
            readSession.mount("http://", HTTPAdapter(max_retries=retry_strategy))
            readInventoryResponse = readSession.get(
                "http://{}:8082/read_inventory".format(INVENTORY)
            )

            assert readInventoryResponse.status_code == 200
            readSession.close()

            availItems = [(itemId, count) for itemId, count in readInventoryResponse.json().items() if count>0]

            if num is not None:
                logs('Recommendation', 'Successfully returning specified number of items to customer')
                return jsonify(
                    {
                        availItems[i][0]: availItems[i][1]
                        for i in range(min(num, len(availItems)))
                    }
                )
            else:
                logs('Recommendation', 'Successfully returning all items to customer')
                return jsonify({itemId: count for itemId, count in availItems})

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
    app.run(port=8086, host="0.0.0.0")