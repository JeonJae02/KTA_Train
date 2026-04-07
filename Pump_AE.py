# Pump_AE.py
import torch
import torch.nn as nn

class PumpAutoencoder(nn.Module):
    def __init__(self, input_dim):
        super(PumpAutoencoder, self).__init__()
        
        # 데이터를 압축하는 과정 (11개 -> 4개)
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 8),
            nn.ReLU(),
            nn.Linear(8, 4),
            nn.ReLU()
        )
        
        # 데이터를 다시 복원하는 과정 (4개 -> 11개)
        self.decoder = nn.Sequential(
            nn.Linear(4, 8),
            nn.ReLU(),
            nn.Linear(8, input_dim)
        )

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded