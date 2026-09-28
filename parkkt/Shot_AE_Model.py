# parkkt/Shot_AE_Model.py
import torch.nn as nn


class ShotAutoencoder(nn.Module):
    """Shot-level 피처용 오토인코더.

    Cycle 쪽 `CycleAutoencoder`(23 -> 32 -> 16 -> 8)를 그대로 쓰면 안 된다.
    입력이 10개뿐인데 은닉층이 32까지 부풀고 병목이 8이면, 병목이 입력과 맞먹어서
    **압축이 일어나지 않는다.** 실제로 그렇게 돌렸을 때 Valid Loss 가 0.0004 까지
    떨어졌는데, 이건 잘 배운 게 아니라 값을 그대로 통과시켰다는 뜻이었다.

    그래서 층을 단조적으로 좁히고 병목을 입력의 1/3 수준으로 잡는다.

        10 -> 8 -> 5 -> 3 -> 5 -> 8 -> 10
    """

    def __init__(self, input_dim, bottleneck=3):
        super().__init__()
        h1 = max(bottleneck + 1, int(input_dim * 0.8))
        h2 = max(bottleneck + 1, int(input_dim * 0.5))
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, h1), nn.ReLU(),
            nn.Linear(h1, h2), nn.ReLU(),
            nn.Linear(h2, bottleneck), nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(bottleneck, h2), nn.ReLU(),
            nn.Linear(h2, h1), nn.ReLU(),
            nn.Linear(h1, input_dim),
        )

    def forward(self, x):
        return self.decoder(self.encoder(x))
