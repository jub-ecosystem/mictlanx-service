from mictlanx.logger.log import Log
import os 


MICTLANX_ROUTER_LOG_NAME     = os.environ.get("MICTLANX_ROUTER_LOG_NAME","mictlanx-peer-managera-0")
MICTLANX_ROUTER_LOG_INTERVAL = int(os.environ.get("MICTLANX_ROUTER_LOG_INTERVAL","24"))
MICTLANX_ROUTER_LOG_WHEN     = os.environ.get("MICTLANX_ROUTER_LOG_WHEN","h")
MICTLANX_ROUTER_LOG_SHOW     = bool(int(os.environ.get("MICTLANX_ROUTER_LOG_SHOW","1")))
LOG_PATH                     = os.environ.get("LOG_PATH","/log")
log                          = Log(
        name                   = MICTLANX_ROUTER_LOG_NAME,
        console_handler_filter = lambda x: MICTLANX_ROUTER_LOG_SHOW,
        interval               = MICTLANX_ROUTER_LOG_INTERVAL,
        when                   = MICTLANX_ROUTER_LOG_WHEN,
        path                   = LOG_PATH
)

async def run_rm(ph):
    # global replicas_map
    print("RUUUUUUUUUN_RUM<MMM")
    try:
        # async with peer_healer_rwlock.writer_lock:
            peers = ph.peers
            ball_ids_global = []
            raw_map = []
            for peer in peers:
                state_result = peer.get_state()
                # print("STATE_RESUTKL", state_result,peer.peer_id)
                
                if state_result.is_ok:
                    response = state_result.unwrap()
                    # response.nodes[0].node_id
                    for key,context in response.balls.items():
                        raw_map.append((key, context.locations))
                    ball_ids = list(response.balls.keys())
                    ball_ids_global.extend(ball_ids)
            replicas_map = {**replicas_map, **dict(raw_map)}
            print(ball_ids_global)

    except Exception as e:
        log.error(str(e))