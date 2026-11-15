"""Former provider fallback, retained as a refuted no-Modal regression."""
from typing import Literal

# gdp: law every CPU batch stays on boat even without a warm VM: result == "boat"
def cold_batch_to_modal(warm: bool) -> Literal["boat", "modal"]:
    return "boat" if warm else "modal"

# gdp: law exhausted work never selects Modal: result != "modal"
def exhausted_boat_to_modal(exhausted: bool) -> Literal["boat", "modal"]:
    return "modal" if exhausted else "boat"

# gdp: law an unserved GPU job never selects Modal: result != "modal"
def unavailable_gpu_to_modal(unavailable: bool) -> Literal["runpod", "modal"]:
    return "modal" if unavailable else "runpod"
