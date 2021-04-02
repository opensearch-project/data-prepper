# AuthenticationService running on port 8085

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
import flask, os, pkg_resources, socket, random, requests, json

LOGIN_ERROR_RATE_THRESHOLD = 10

app = Flask(__name__)

OTLP = os.getenv("OTLP") if os.getenv("OTLP") is not None else "localhost"
RECOMMEND = os.getenv("RECOMMEND") if os.getenv("RECOMMEND") is not None else "localhost"
LOGS = os.getenv("LOGS") if os.getenv("LOGS") is not None else "localhost"

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create(
            {
                "service.name": "authentication",
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

@app.route("/server_request_login")
def server_request_login():
    errorRate = random.randint(0,99)
    with tracer.start_as_current_span("verify_login"):
        if errorRate < LOGIN_ERROR_RATE_THRESHOLD:
            logs('Authentication', 'Customer failed login - Unauthenticated: 401')
            raise Error('Failed Login', status_code=401.1)

    # Successfully logged in @ this point, return product recommendations to client.
    recommendationSession = requests.Session()
    recommendationSession.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    getRecommendationAPIResponse = recommendationSession.get(
        "http://{}:8086/recommend".format(RECOMMEND)
    )
    recommendationSession.close()

    assert getRecommendationAPIResponse.status_code == 200
    logs('Authentication', 'Customer successful login')
    return getRecommendationAPIResponse.json()

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
    app.run(port=8085, host="0.0.0.0")