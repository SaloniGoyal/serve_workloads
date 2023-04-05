import time
import asyncio
import requests
import itertools
from typing import Dict
from fastapi import FastAPI
from aiohttp import TraceConfig
from aiohttp_retry import RetryClient, ExponentialRetry

from chaos_test.constants import RECEIVER_KILL_KEY, KillOptions

from ray import serve
from ray.util.metrics import Counter, Gauge
from ray._private.utils import run_background_task


# These options should get overwritten in the Serve config with a real
# authentication token and URL.
DEFAULT_BEARER_TOKEN = "default"
DEFAULT_RECEIVER_URL = "http://localhost:8000/fail"
DEFAULT_RECEIVER_SERVICE_ID = "default"
DEFAULT_COOKIE = ""
DEFAULT_KILL_INTERVAL_S = 300  # In seconds
DEFAULT_MAX_QPS = 100


app = FastAPI()


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 0})
@serve.ingress(app)
class Router:
    def __init__(self, pinger_handle, reaper_handle):
        self.pinger = pinger_handle
        self.reaper = reaper_handle

    @app.get("/")
    def root(self) -> str:
        return "Hi there!"

    @app.get("/start")
    async def start(self):
        await self.stop_pinger()
        await self.stop_reaper()
        return "Started Pinger and Reaper!"

    @app.get("/start-pinger")
    async def stop_pinger(self) -> str:
        await (await self.pinger.start.remote())
        return "Started Pinger!"

    @app.get("/start-reaper")
    async def stop_reaper(self) -> str:
        await (await self.reaper.start.remote())
        return "Started Reaper!"

    @app.get("/stop")
    async def stop(self):
        await self.stop_pinger()
        await self.stop_reaper()
        return "Stopped Pinger and Reaper!"

    @app.get("/stop-pinger")
    async def stop_pinger(self) -> str:
        await (await self.pinger.stop.remote())
        return "Stopped Pinger!"

    @app.get("/stop-reaper")
    async def stop_reaper(self) -> str:
        await (await self.reaper.stop.remote())
        return "Stopped Reaper!"

    @app.get("/info")
    async def get_info(self):
        pinger_info = (await (await self.pinger.get_info.remote())).copy()
        reaper_info = (await (await self.reaper.get_info.remote())).copy()
        pinger_info.update(reaper_info)
        return pinger_info


@serve.deployment(
    num_replicas=1,
    user_config={
        "receiver_url": DEFAULT_RECEIVER_URL,
        "bearer_token": DEFAULT_BEARER_TOKEN,
        "max_qps": DEFAULT_MAX_QPS,
    },
    ray_actor_options={"num_cpus": 0},
)
class Pinger:
    def __init__(self):
        self.receiver_url = ""
        self.bearer_token = ""
        self.max_qps = 0
        self.request_loop_task = None
        self._initialize_stats()
        self._initialize_metrics()
        self.client = self._create_http_client()
        self.pending_requests = set()

    def reconfigure(self, config: Dict):

        new_receiver_url = config.get("receiver_url", DEFAULT_RECEIVER_URL)
        print(
            f'Changing receiver URL from "{self.receiver_url}" to "{new_receiver_url}"'
        )
        self.receiver_url = new_receiver_url

        new_bearer_token = config.get("bearer_token", DEFAULT_BEARER_TOKEN)
        print(
            f'Changing bearer token from "{self.bearer_token}" to "{new_bearer_token}"'
        )
        self.bearer_token = new_bearer_token

        new_max_qps = config.get("max_qps", DEFAULT_MAX_QPS)
        print(f'Changing max QPS from "{self.max_qps}" to "{new_max_qps}"')
        self.max_qps = new_max_qps

        self.start()

    async def run_request_loop(self):
        await self._drain_requests()
        send_interval_s = 1 / self.max_qps

        while True:
            json_payload = {RECEIVER_KILL_KEY: KillOptions.SPARE}

            try:
                start_time = time.time()

                self.pending_requests.add(
                    self.client.post(
                        self.receiver_url,
                        headers={"Authorization": f"Bearer {self.bearer_token}"},
                        json=json_payload,
                        timeout=3,
                    )
                )

                # Spend half the send interval waiting for pending requests.
                # Spend the rest on processing the responses.
                done, pending = await asyncio.wait(
                    self.pending_requests, timeout=send_interval_s / 2
                )
                self.pending_requests = pending
                self.num_pending_requests = len(self.pending_requests)
                self.pending_requests_gauge.set(len(self.pending_requests))

                for task in done:
                    response = await task
                    self.request_counter.inc()
                    status_code = response.status
                    if status_code == 200:
                        self._count_successful_request()
                        self.success_counter.inc()
                    else:
                        self._count_failed_request(
                            status_code, reason=(await response.text())
                        )
                        self.fail_counter.inc()
                        self._increment_error_counter(status_code)
                    if self.current_num_requests % 100 == 0:
                        print(
                            f"{time.strftime('%b %d – %l:%M%p: ')}"
                            f"Sent {self.current_num_requests} "
                            f'requests to "{self.receiver_url}".'
                        )
            except Exception as e:
                self._count_failed_request(-1, reason=repr(e))
                self.fail_counter.inc()
                print(
                    f"{time.strftime('%b %d – %l:%M%p: ')}"
                    f"Got exception: \n{repr(e)}"
                )

            send_interval_remaining_s = send_interval_s - (time.time() - start_time)
            if send_interval_remaining_s > 0:
                await asyncio.sleep(send_interval_remaining_s)

    def start(self):
        if self.request_loop_task is not None:
            print("Called start() while Pinger is already running. Nothing changed.")
        else:
            self.request_loop_task = run_background_task(self.run_request_loop())
            print("Started Pinger. Call stop() to stop.")

    async def stop(self):
        if self.request_loop_task is not None:
            self.request_loop_task.cancel()
            self.request_loop_task = None
            await self._drain_requests()
            print("Stopped Pinger. Call start() to start.")
        else:
            print("Called stop() while Pinger was already stopped. Nothing changed.")
        self._reset_current_counters()

    def get_info(self):
        info = {
            "Pinger running": self.request_loop_task is not None,
            "Target URL": self.receiver_url,
            "Total number of requests": self.total_num_requests,
            "Total successful requests": self.total_successful_requests,
            "Total failed requests": self.total_failed_requests,
            "Current number of requests": self.current_num_requests,
            "Current successful requests": self.current_successful_requests,
            "Current failed requests": self.current_failed_requests,
            "Number of pending requests": self.num_pending_requests,
            "Failed response counts": self.failed_response_counts,
            "Failed response reasons": self.failed_response_reasons,
        }
        return info

    def _initialize_stats(self):
        self.total_num_requests = 0
        self.total_successful_requests = 0
        self.total_failed_requests = 0
        self.current_num_requests = 0
        self.current_successful_requests = 0
        self.current_failed_requests = 0
        self.num_pending_requests = 0
        self.failed_response_counts = dict()
        self.failed_response_reasons = dict()

    def _reset_current_counters(self):
        self.current_num_requests = 0
        self.current_successful_requests = 0
        self.current_failed_requests = 0
        self.current_kill_requests = 0
        self.num_pending_requests = 0

    def _initialize_metrics(self):
        self.request_counter = Counter(
            "pinger_num_requests",
            description="Number of requests.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Pinger"})

        self.success_counter = Counter(
            "pinger_num_requests_succeeded",
            description="Number of successful requests.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Pinger"})

        self.fail_counter = Counter(
            "pinger_num_requests_failed",
            description="Number of failed requests.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Pinger"})

        self.latency_gauge = Gauge(
            "pinger_request_latency",
            description="Latency of last successful request.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Pinger"})

        self.pending_requests_gauge = Gauge(
            "pinger_pending_requests",
            description="Number of requests being processed by the Receiver.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Pinger"})

        self.http_error_counters = dict()

        for error_code in itertools.chain(range(400, 419), range(500, 512)):
            self.http_error_counters[error_code] = Counter(
                f"pinger_{error_code}_http_error",
                description=f"Number of {error_code} HTTP response errors received.",
                tag_keys=("class",),
            ).set_default_tags({"class": "Pinger"})

        self.http_error_fallback_counter = Counter(
            "pinger_fallback_http_error",
            description="Number of other HTTP response errors received.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Pinger"})

    def _increment_error_counter(self, status_code: int):
        """Increments the error counter corresponding to the status_code."""

        if status_code in self.http_error_counters:
            self.http_error_counters[status_code].inc()
        else:
            self.http_error_fallback_counter.inc()

    def _count_successful_request(self):
        self.total_num_requests += 1
        self.total_successful_requests += 1
        self.current_num_requests += 1
        self.current_successful_requests += 1

    def _count_failed_request(self, status_code: int, reason: str = ""):
        self.total_num_requests += 1
        self.total_failed_requests += 1
        self.current_num_requests += 1
        self.current_failed_requests += 1
        self.failed_response_counts[status_code] = (
            self.failed_response_counts.get(status_code, 0) + 1
        )
        if status_code in self.failed_response_reasons:
            self.failed_response_reasons[status_code].add(reason)
        else:
            self.failed_response_reasons[status_code] = set([reason])

    def _create_http_client(self):

        # From https://stackoverflow.com/a/63925153
        async def on_request_start(session, trace_config_ctx, params):
            trace_config_ctx.start = asyncio.get_event_loop().time()

        async def on_request_end(session, trace_config_ctx, params):
            latency = asyncio.get_event_loop().time() - trace_config_ctx.start
            self.latency_gauge.set(latency)

        trace_config = TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)

        return RetryClient(
            retry_options=ExponentialRetry(attempts=5, start_timeout=0.1, factor=2),
            trace_configs=[trace_config],
        )

    async def _drain_requests(self):
        await asyncio.gather(*self.pending_requests)
        self.pending_requests.clear()


@serve.deployment(
    num_replicas=1,
    user_config={
        "receiver_url": DEFAULT_RECEIVER_URL,
        "bearer_token": DEFAULT_BEARER_TOKEN,
        "kill_interval_s": DEFAULT_KILL_INTERVAL_S,
    },
    ray_actor_options={"num_cpus": 0},
)
class Reaper:
    def __init__(self):
        self.receiver_url = ""
        self.bearer_token = ""
        self.kill_interval_s = float("inf")
        self.kill_loop_task = None
        self._initialize_stats()
        self.kill_counter = Counter(
            "pinger_num_kill_requests_sent",
            description="Number of kill requests sent.",
            tag_keys=("class",),
        ).set_default_tags({"class": "Reaper"})

    def reconfigure(self, config: Dict):

        new_receiver_url = config.get("receiver_url", DEFAULT_RECEIVER_URL)
        print(
            f'Changing receiver URL from "{self.receiver_url}" to "{new_receiver_url}"'
        )
        self.receiver_url = new_receiver_url

        new_bearer_token = config.get("bearer_token", DEFAULT_BEARER_TOKEN)
        print(
            f'Changing bearer token from "{self.bearer_token}" to "{new_bearer_token}"'
        )
        self.bearer_token = new_bearer_token

        new_kill_interval = config.get("kill_interval_s", DEFAULT_KILL_INTERVAL_S)
        print(
            f"Changing kill interval from {self.kill_interval_s} to {new_kill_interval} seconds."
        )
        self.kill_interval_s = new_kill_interval

        self.start()

    async def kill_loop(self):
        while True:
            print(
                f"Sleeping for {self.kill_interval_s} seconds before sending "
                "next kill request."
            )
            await asyncio.sleep(self.kill_interval_s)
            json_payload = {RECEIVER_KILL_KEY: KillOptions.KILL}
            try:
                print("Sending kill request.")
                requests.post(
                    self.receiver_url,
                    headers={"Authorization": f"Bearer {self.bearer_token}"},
                    json=json_payload,
                    timeout=3,
                )
            except Exception as e:
                print(
                    "Got following exception when sending kill " f"request: {repr(e)}"
                )
            self.kill_counter.inc()

    def start(self):
        if self.kill_loop_task is not None:
            print("Called start() while Reaper is already running. Nothing changed.")
        else:
            self.kill_loop_task = run_background_task(self.kill_loop())
            print("Started Reaper. Call stop() to stop.")

    def stop(self):
        if self.kill_loop_task is not None:
            self.kill_loop_task.cancel()
            self.kill_loop_task = None
            print("Stopped Reaper. Call start() to start.")
        else:
            print("Called stop() while Reaper was already stopped. Nothing changed.")
        self.current_kill_requests = 0

    def get_info(self) -> Dict[str, int]:
        return {
            "Reaper running": self.kill_loop_task is not None,
            "Total kill requests": self.total_kill_requests,
            "Current kill requests": self.current_kill_requests,
        }

    def _initialize_stats(self):
        self.total_kill_requests = 0
        self.current_kill_requests = 0


@serve.deployment(
    num_replicas=1,
    user_config={
        "receiver_service_id": DEFAULT_RECEIVER_SERVICE_ID,
        "cookie": DEFAULT_COOKIE,
    },
    ray_actor_options={"num_cpus": 0},
)
class ReceiverHelmsman:
    def __init__(self):
        self.receiver_service_id = ""
        self.cookie = ""
        self.manage_loop_task = None
        self._initialize_metrics()
        self.latest_receiver_status = None

    def reconfigure(self, config: Dict):
        new_receiver_service_id = config.get(
            "receiver_service_id", DEFAULT_RECEIVER_SERVICE_ID
        )
        print(
            f'Changing receiver service ID from "{self.receiver_service_id}" to "{new_receiver_service_id}"'
        )
        self.receiver_service_id = new_receiver_service_id

        new_cookie = config.get("cookie", DEFAULT_COOKIE)
        print(f'Changing cookie from "{self.cookie}" to "{new_cookie}"')
        self.cookie = new_cookie

        self.start()

    async def run_manage_loop(self):
        while True:
            self._log_receiver_status()
            await asyncio.sleep(5)

    def start(self):
        if self.manage_loop_task is not None:
            print(
                "Called start() while ReceiverHelmsman is already running. Nothing changed."
            )
        else:
            self.manage_loop_task = run_background_task(self.run_manage_loop())
            print("Started ReceiverHelmsman. Call stop() to stop.")

    def stop(self):
        if self.manage_loop_task is not None:
            self.manage_loop_task.cancel()
            self.manage_loop_task = None
            print("Stopped ReceiverHelmsman. Call start() to start.")
        else:
            print(
                "Called stop() while ReceiverHelmsman was already stopped. Nothing changed."
            )
        self.current_kill_requests = 0

    def _initialize_metrics(self):
        self.receiver_status_gauge = Gauge(
            "pinger_request_latency",
            description="Latency of last successful request.",
            tag_keys=("class", "status"),
        ).set_default_tags({"class": "ReceiverHelmsman"})

    def _log_receiver_status(self):
        get_url = f"https://console.anyscale-staging.com/api/v2/services-v2/{self.receiver_service_id}"
        try:
            response = requests.get(get_url, headers={"Cookie": self.cookie})
            receiver_status = response.json()["current_state"]
            if (
                self.latest_receiver_status is not None
                and self.latest_receiver_status != receiver_status
            ):
                self.receiver_status_gauge.set(
                    0,
                    tags={
                        "class": "ReceiverHelmsman",
                        "status": self.latest_receiver_status,
                    },
                )
            self.receiver_status_gauge.set(
                1, tags={"class": "ReceiverHelmsman", "status": receiver_status}
            )
            self.latest_receiver_status = receiver_status
        except Exception as e:
            print(f"Got exception when getting Receiver service's status: {repr(e)}")


graph = Router.bind(Pinger.bind(), Reaper.bind())
