import json
import traceback
import logging
import threading
import time
from collections import defaultdict
from ..core.ssl_context import context
from ..core.websocket_mngr import WebsocketManager as BaseWebsocketManager

from hyperliquid.utils.types import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Subscription,
    Tuple,
    WsMsg,
)

ActiveSubscription = NamedTuple(
    "ActiveSubscription",
    [("callback", Callable[[Any], None]), ("subscription_id", int)],
)


def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"]}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"]}'
    elif subscription["type"] == "userEvents":
        return "userEvents"


def identifier_to_sub(identifier: str) -> {}:
    if identifier == "allMids":
        return {"type": "allMids"}
    elif identifier.startswith("l2Book"):
        return {"type": "l2Book", "coin": identifier.split(":")[1]}
    elif identifier.startswith("trades"):
        return {"type": "trades", "coin": identifier.split(":")[1]}
    elif identifier == "userEvents":
        return {"type": "userEvents"}


def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    if ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"]}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"]}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "post":
        return "post"


class WebsocketManager(BaseWebsocketManager):
    def __init__(self, base_url, timeout_thresh=30, sub_timeout=30):
        self.sub_timeout = sub_timeout
        self.subscription_id_counter = 0
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(
            list
        )
        self.confirmed_subs = {}
        self.post_listeners = []
        self.ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ping_sender = threading.Thread(target=self.send_ping)
        self.ping_sender.start()
        self.post_id = 0
        BaseWebsocketManager.__init__(
            self, self.ws_url, timeout_threshold=timeout_thresh, warn_threshold=5
        )
        # self.test_closer = threading.Thread(target=self.test_close)
        # self.test_closer.start()
        self.unconf = threading.Thread(target=self.handle_unconformed_subs)
        self.unconf.start()

    def handle_unconformed_subs(self):
        while True:
            time.sleep(5)
            for identifier, data in self.confirmed_subs.items():
                if (
                    not data["confirmed"]
                    and data["last_sub"] < time.time() - self.sub_timeout
                ):
                    logging.info(f"Never got sub resp, resubscribing to {identifier}")
                    sub = identifier_to_sub(identifier)
                    self.send_sub(sub)

    def test_close(self):
        while True:
            time.sleep(60)
            print("Testing close")
            self.ws.close()

    def post(self, data) -> int:
        self.post_id += 1
        request = {
            "method": "post",
            "id": self.post_id,
            "request": data,
        }
        self.ws.send(json.dumps(request))
        return self.post_id

    def send_ping(self):
        while True:
            time.sleep(50)
            logging.debug("Websocket sending ping")
            if self.ws.sock and self.ws.sock.connected:
                self.ws.send(json.dumps({"method": "ping"}))

    def on_subscription_response(self, ws_msg: WsMsg):
        if "data" in ws_msg and "subscription" in ws_msg["data"]:
            identifier = subscription_to_identifier(ws_msg["data"]["subscription"])
            self.confirmed_subs[identifier]["confirmed"] = True

    def on_message(self, _ws, message):
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        logging.debug(f"on_message {message}")
        ws_msg: WsMsg = json.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        if ws_msg["channel"] == "subscriptionResponse":
            logging.info(f"Confirmed sub {identifier}")
            return self.on_subscription_response(ws_msg)
        elif (
            identifier in self.confirmed_subs
            and not self.confirmed_subs[identifier]["confirmed"]
        ):
            logging.info(f"Confirmed sub {identifier}")
            self.confirmed_subs[identifier]["confirmed"] = True
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        if identifier is None:
            logging.debug("Websocket not handling empty message")
            return
        if identifier == "post":
            for listener in self.post_listeners:
                listener(ws_msg)
            return
        active_sub = self.active_subscriptions.get(identifier)
        if active_sub is None:
            logging.info(
                f"Websocket message from an unexpected subscription: {message}, {identifier}"
            )
        else:
            try:
                active_sub.callback(ws_msg)
            except Exception as e:
                logging.error(f"Error in callback: {e}")
                traceback.print_exc()

    def subscribe_post_ws(self, callback) -> int:
        self.post_listeners.append(callback)

    def subscribe(
        self,
        subscription: Subscription,
        callback: Callable[[Any], None],
        subscription_id: Optional[int] = None,
    ) -> int:
        self.add_subscription(subscription, callback, subscription_id)
        self.send_sub(subscription)
        return subscription_id

    def send_sub(self, sub, retry=3):
        identifier = subscription_to_identifier(sub)
        count = 0
        while count < retry:
            try:
                self.confirmed_subs[identifier] = {
                    "confirmed": False,
                    "last_sub": time.time(),
                }
                self.ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
                break
            except Exception as e:
                logging.error(
                    f"Error resubscribing to {identifier}: {e}, retrying in 10s"
                )
                count += 1
                time.sleep(10)

    def add_subscription(
        self, subscription: str, callback, subscription_id: Optional[int] = None
    ):
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        identifier = subscription_to_identifier(subscription)
        if subscription["type"] == "userEvents":
            # TODO: ideally the userEvent messages would include the user so that we can support multiplexing them
            if len(self.active_subscriptions[identifier]) != 0:
                raise NotImplementedError(
                    "Cannot subscribe to UserEvents multiple times"
                )
        self.active_subscriptions[identifier] = ActiveSubscription(
            callback, subscription_id
        )

    def subscribe_queued(self):
        for identifier in self.active_subscriptions:
            sub = identifier_to_sub(identifier)
            logging.info(f"Resubscribing to {identifier}")
            self.send_sub(sub)
            time.sleep(0.1)

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [
            x for x in active_subscriptions if x.subscription_id != subscription_id
        ]
        if len(new_active_subscriptions) == 0:
            self.ws.send(
                json.dumps({"method": "unsubscribe", "subscription": subscription})
            )
        self.active_subscriptions[identifier] = new_active_subscriptions
        return len(active_subscriptions) != len(active_subscriptions)

    def resubscribe(self):
        self.subscribe_queued()
