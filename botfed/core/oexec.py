from typing import TypedDict, List


class OrderResp(TypedDict):
    oid: str
    cloid: str
    status: str
    update_time: int
    error_msg: str
