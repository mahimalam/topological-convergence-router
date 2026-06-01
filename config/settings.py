from dataclasses import dataclass

@dataclass
class TopologyConfig:
    max_concurrent_nodes: int = 50
    divergence_threshold: float = 0.02
    execution_timeout_ms: int = 500

CONFIG = TopologyConfig()
