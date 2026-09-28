# parkkt/Cycle_AE.py
import torch.nn as nn


class CycleAutoencoder(nn.Module):
    """Cycle-level 피처용 오토인코더.

    기존 Pump_AE.PumpAutoencoder(14 -> 8 -> 4)보다 입력 차원이 커져서(약 30개)
    한 단계 더 깊게 잡았다.
    """

    def __init__(self, input_dim, bottleneck=8):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 32), nn.ReLU(),
            nn.Linear(32, 16), nn.ReLU(),
            nn.Linear(16, bottleneck), nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(bottleneck, 16), nn.ReLU(),
            nn.Linear(16, 32), nn.ReLU(),
            nn.Linear(32, input_dim),
        )

    def forward(self, x):
        return self.decoder(self.encoder(x))
