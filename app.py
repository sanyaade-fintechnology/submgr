import zmq
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
from zmapi.codes import error
from zmapi.zmq import SockRecvPublisher
from zmapi.logging import setup_root_logger
from zmapi.utils import check_missing
from zmapi import SubscriptionDefinition
import uuid
from sortedcontainers import SortedDict
from collections import defaultdict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime
from copy import deepcopy

################################## CONSTANTS ##################################

MODULE_NAME = "submgr"

################################# EXCEPTIONS ##################################

class InvalidArgumentsException(Exception):
    pass

################################### GLOBALS ###################################

L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.pub_bytes = defaultdict(lambda: 0)

# info for each client_id, subscriptions are indexed by full ticker_id
def new_clients_value():
    d = {"subscriptions": defaultdict(SubscriptionDefinition)}
    return d
g.clients = defaultdict(new_clients_value)

# info for each full ticker_id, subscriptions are indexed by client_id
def new_tickers_value():
    d = {"subscriptions": defaultdict(SubscriptionDefinition)}
    return d
g.tickers = defaultdict(new_tickers_value)

################################### HELPERS ###################################

def split_message(msg_parts):
    separator_idx = None
    for i, part in enumerate(msg_parts):
        if not part:
            separator_idx = i
            break
    if not separator_idx:
        raise ValueError("ident separator not found")
    ident = msg_parts[:separator_idx]
    msg = msg_parts[separator_idx+1]
    return ident, msg

def ident_to_str(ident):
    return "/".join([x.decode("latin-1").replace("/", "\/") for x in ident])

def partition(coll, n, step=None, complete_only=False):
    if step is None:
        step = n
    for i in range(0, len(coll), step):
        items_over = i + n - len(coll)
        if items_over > 0 and complete_only:
            return
        else:
            yield coll[i:i+n]

def merge_with(f, dicts):
    res = defaultdict(lambda: [])
    for d in dicts:
        for k, v in d.items():
            res[k].append(v)
    for k, v in res.items():
        res[k] = f(v)
    return dict(res)

# mutates msg
def pack_msg(msg : dict):
    if not msg.get("msg_id"):
        msg["msg_id"] = str(uuid.uuid4())
    return (" " + json.dumps(msg)).encode()

def unpack_msg(msg : bytes):
    return json.loads(msg.decode())

async def send_recv_command_raw(sock, msg):
    msg_bytes = pack_msg(msg)
    msg_id = msg["msg_id"]
    await sock.send_multipart([b"", msg_bytes])
    # this will wipe the message queue
    while True:
        msg_parts = await sock.recv_multipart()
        try:
            msg = json.loads(msg_parts[-1].decode())
            if msg["msg_id"] == msg_id:
                break
        except:
            pass
    error.check_message(msg)
    return msg["content"]

###############################################################################

async def reap_client(client, d):
    ticker_ids = list(d["subscriptions"].keys())
    for ticker_id in ticker_ids:
        msg = await update_subscription(
                client, ticker_id, SubscriptionDefinition())
        error.check_message(msg)
    del g.clients[client]

async def run_client_reaper():
    while True:
        clients = list(g.clients.keys())
        for client in clients:
            d = g.clients[client]
            d["health"] -= 1
            if d["health"] <= 0:
                L.info("reaping client {}".format(client))
                try:
                    await reap_client(client, d)
                except RemoteException as err:
                    L.exception("error when reaping client {}:".format(client))
                L.info("reaped client {}".format(client))
        await asyncio.sleep(g.reaper_interval)

###############################################################################

async def send_error(ident, msg_id, ecode, msg=None):
    msg = error.gen_error(ecode, msg)
    msg["msg_id"] = msg_id
    msg = " " + json.dumps(msg)
    msg = msg.encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg])

async def run_ctl_handler():
    L.info("running ctl handler ...")
    while True:
        msg_parts = await g.sock_ctl.recv_multipart()
        try:
            ident, msg = split_message(msg_parts)
        except ValueError as err:
            L.error(str(err))
            continue
        client = ident_to_str(ident)
        if client not in g.clients:
            g.clients[client] = {
                "subscriptions": {},
                "health": g.reaper_max_heartbeats
            }
        g.clients[client]["health"] = g.reaper_max_heartbeats
        if len(msg) == 0:
            # Ping message is not forwarded to upstream branches.
            # Ping messages are used here to signal that the client is alive.
            # Dead clients' subscriptions are automatically removed.
            await g.sock_ctl.send_multipart(msg_parts)
            continue
        msg = unpack_msg(msg)
        create_task(handle_ctl_msg_1(ident, msg, client))

async def get_status(ident, msg):
    msg_orig = msg
    msg_bytes = pack_msg(msg)
    res = []
    for name in sorted(g.up):
        d = g.up[name]
        await d["sock_deal"].send_multipart(ident + [b"", msg_bytes])
        msg_parts = await d["sock_deal_pub"].poll_for_msg_id(msg["msg_id"])
        msg = unpack_msg(msg_parts[-1])
        error.check_message(msg)
        content = msg["content"]
        res += content
    status = {
        "name": MODULE_NAME,
        "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
        "pub_bytes": g.pub_bytes,
        "num_clients": len(g.clients),
        "num_subscribed_tickers": sum([bool(x["subscriptions"])
                                      for x in g.tickers.values()]),
        "reaper_interval": g.reaper_interval,
        "reaper_max_heartbeats": g.reaper_max_heartbeats,
    }
    content = [status, res]
    msg_orig["content"] = content
    await g.sock_ctl.send_multipart(ident + [b"", pack_msg(msg_orig)])

def sum_and_maybe_booleanize(xs):
    res = sum(xs)
    if type(xs[0]) is bool:
        return bool(res)
    return res

async def update_subscription(client, ticker_id, sd, msg_id=None):
    t_d = g.tickers[ticker_id]["subscriptions"]
    c_d = g.clients[client]["subscriptions"]
    old_sub = merge_with(sum_and_maybe_booleanize,
                         [x.__dict__ for x in t_d.values()])
    old_sub = SubscriptionDefinition(**old_sub)
    c_d[ticker_id] = sd
    t_d[client] = sd
    new_sub = merge_with(sum_and_maybe_booleanize,
            [x.__dict__ for x in t_d.values()])
    new_sub = SubscriptionDefinition(**new_sub)
    if new_sub.empty():
        c_d.pop(ticker_id, None)
        t_d.pop(client, None)
    print(client, ticker_id)
    pprint(old_sub)
    pprint(new_sub)
    if old_sub == new_sub:
        content = {}
        content["trades"] = "registered to manager, no change required"
        content["order_book"] = "registered to manager, no change required"
        return {"content": content}
    L.info("{}: {} ({} subscribers)".format(ticker_id, new_sub, len(t_d)))
    spl = ticker_id.split("/")
    connector = spl[0]
    raw_ticker_id = "/".join(spl[1:])
    up_d = g.up[connector]
    content = {"ticker_id": raw_ticker_id}
    content.update(new_sub.__dict__)
    msg = {
        "content": content,
        "msg_id": msg_id,
        "command": "modify_subscription",
    }
    await up_d["sock_deal"].send_multipart([b"", pack_msg(msg)])
    msg_parts = await up_d["sock_deal_pub"].poll_for_msg_id(msg["msg_id"])
    msg = json.loads(msg_parts[-1].decode())
    return msg

async def handle_modify_subscription(ident, client, msg):
    msg_id = msg["msg_id"]
    msg["command"] = "subscribe"
    connector = msg.pop("connector")
    content = msg["content"]
    raw_ticker_id = content["ticker_id"]
    ticker_id = connector + "/" + raw_ticker_id
    old_sub = g.tickers[ticker_id]["subscriptions"][client]
    new_sub = deepcopy(old_sub)
    new_sub.update(content)
    if old_sub != new_sub:
        msg = await update_subscription(client, ticker_id, new_sub)
    if old_sub == new_sub:
        content = {"trades": "no change", "order_book": "no change"}
        msg = {"content": content}
    msg["msg_id"] = msg_id
    await g.sock_ctl.send_multipart(ident + [b"", pack_msg(msg)])

async def handle_unsubscribe(ident, client, msg):
    content = msg["content"]
    content.update(SubscriptionDefinition().__dict__)
    await handle_modify_subscription(ident, client, msg)

async def fwd_message_no_change(ident, msg):
    connector = msg.pop("connector")
    if not connector:
        raise InvalidArgumentsException("connector not defined")
    d = g.up[connector]
    await d["sock_deal"].send_multipart(ident + [b"", pack_msg(msg)])
    msg_parts = await d["sock_deal_pub"].poll_for_msg_id(msg["msg_id"])
    await g.sock_ctl.send_multipart(ident + [b"", msg_parts[-1]])

async def handle_ctl_msg_1(ident, msg, client):
    msg_id = msg["msg_id"]
    cmd = msg["command"]
    debug_msg = "ident={}, command={}, msg_id={}"
    debug_msg = debug_msg.format(ident_to_str(ident),
                                 cmd,
                                 msg_id)
    if "connector" in msg:
        debug_msg += " connector={}".format(msg["connector"])
    L.debug("> " + debug_msg)
    try:
        if cmd not in ["get_status"]:
            check_missing("connector", msg)
        # TODO: add support for `add_connector` and `remove_connector` commands
        if cmd == "get_status":
            await get_status(ident, msg)
        elif cmd == "modify_subscription":
            await handle_modify_subscription(ident, client, msg)
        elif cmd == "unsubscribe":
            await handle_unsubscribe(ident, client, msg)
        else:
            await fwd_message_no_change(ident, msg)
    except InvalidArgumentsException as e:
        L.exception("invalid arguments on message:")
        await send_error(ident, msg_id, error.ARGS, str(e))
    except Exception as e:
        L.exception("exception on msg_id: {}".format(msg_id))
        await send_error(ident, msg_id, error.GENERIC, str(e))
    L.debug("< " + debug_msg)

###############################################################################

async def run_multiplexing_subscriber(sock_sub, connector : str):
    L.info("running multiplexing subscriber for {} ...".format(connector))
    g.pub_bytes[connector] = 0
    connector_bytes = connector.encode()
    while True:
        msg_parts = await sock_sub.recv_multipart()
        g.pub_bytes[connector] += len(msg_parts[1])
        msg_parts[0] = connector_bytes + b"/" + msg_parts[0]
        await g.sock_pub.send_multipart(msg_parts)

###############################################################################

def parse_args():
    desc = "subscription manager middleware module"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("upstream_addresses", nargs="+",
                        help="pairs of ctl and pub addresses representing "
                             "connected upstream branches")
    parser.add_argument("--reaper-interval", type=int, default=60,
                        help="reaper interval")
    parser.add_argument("--reaper-max-hb", type=int, default=3,
                        help="reaper max heartbeats")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def setup_logging(args):
    setup_root_logger(args.log_level)

async def init_get_connector_name(sock):
    msg = {"command": "get_status"}
    return (await send_recv_command_raw(sock, msg))[-1]["connector_name"]

async def unsubscribe_all(sock):
    msg = { "command": "get_subscriptions" }
    old_subscriptions = await send_recv_command_raw(sock, msg)
    for ticker_id in old_subscriptions:
        msg = {"command": "modify_subscription"}
        content = {"ticker_id": ticker_id}
        content.update(SubscriptionDefinition().__dict__)
        msg["content"] = content
        L.info("unsubscribing old subscription: {}".format(ticker_id))
        await send_recv_command_raw(sock, msg)

async def init_new_upstream_chain(ctl_addr, pub_addr):
    L.info("initializing new upstream chain: CTL={}, PUB={} ..."
           .format(ctl_addr, pub_addr))
    sock_deal = g.ctx.socket(zmq.DEALER)
    sock_deal.setsockopt_string(zmq.IDENTITY, MODULE_NAME)
    sock_deal.connect(ctl_addr)
    sock_sub = g.ctx.socket(zmq.SUB)
    sock_sub.connect(pub_addr)
    sock_sub.subscribe(b"")
    name = await init_get_connector_name(sock_deal)
    L.info("{}: ctl={}, pub={}".format(name, ctl_addr, pub_addr))
    await unsubscribe_all(sock_deal)
    g.up[name] = d = {}
    d["sock_deal"] = sock_deal
    d["sock_deal_pub"] = SockRecvPublisher(g.ctx, sock_deal)
    d["sock_sub"] = sock_sub

async def remove_upstream_chain(name):
    L.info("removing upstream chain: {}".format(name))
    d = g.up.pop(name)
    await unsubscribe_all(d["sock_deal"])
    await d["sock_deal_pub"].destroy()
    d["sock_deal"].close()
    d["sock_sub"].close()

async def init_zmq_sockets(args):
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr_down)
    g.up = {}
    pairs = partition(args.upstream_addresses, 2, complete_only=True)
    for ctl_addr, pub_addr in pairs:
        await init_new_upstream_chain(ctl_addr, pub_addr)

def main():
    global L
    args = parse_args()
    g.reaper_interval = args.reaper_interval
    g.reaper_max_heartbeats = args.reaper_max_hb
    setup_logging(args)
    g.loop.run_until_complete(init_zmq_sockets(args))
    tasks = []
    tasks.append(run_ctl_handler())
    tasks.append(run_client_reaper())
    for name, d in g.up.items():
        tasks.append(d["sock_deal_pub"].run())
        tasks.append(run_multiplexing_subscriber(d["sock_sub"], name))
        # tasks.append(create_task(remove_upstream_chain(name)))
    tasks = [create_task(task) for task in tasks]
    g.loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == "__main__":
    main()
