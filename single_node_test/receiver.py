import os
from starlette.requests import Request

from single_node_test.constants import ARTICLE_TEXT_KEY

import ray
from ray import serve

@serve.deployment(
    num_replicas=1,
)
class Receiver:
    def __init__(self):
        print(
            f"Receiver actor starting on node {ray.get_runtime_context().get_node_id()}"
        )

    async def __call__(self, request: Request):
        request_json = await request.json()
        if ARTICLE_TEXT_KEY in request_json:
            article_text = request_json[ARTICLE_TEXT_KEY]
            return {"summary": article_text}
        else:
            return f"(PID: {os.getpid()}) Received request!"


graph = Receiver.bind()
