# models_structure/Pump_Cycle_AE.py
"""펌프 사이클 AE — 14대 공용 조건부 오토인코더 구조. 가중치는 models/pump_cycle/model.pt"""
# 자동 생성 — DB_Log/HDI_MODEL/relay_port/make_port.py. 원본: HDI_MODEL/cycle_ae/model.py
import torch
import torch.nn as nn

FEATURES = ["pump_resid", "r_phys", "t_self", "decel_half_sec", "release_half_sec", "ft_cv", "pt_std", "P0"]
CONDS = ["mu_rel_z", "hz_rel", "dhz_rel", "is_iso", "is_shot", "is_multi"]


class CycleAE(nn.Module):
    def __init__(self, n_feat=len(FEATURES), n_cond=len(CONDS), latent=4, hidden=(32, 16)):
        super().__init__()
        h1, h2 = hidden
        self.enc = nn.Sequential(nn.Linear(n_feat + n_cond, h1), nn.ReLU(), nn.Linear(h1, h2), nn.ReLU(),
                                 nn.Linear(h2, latent))
        self.dec = nn.Sequential(nn.Linear(latent + n_cond, h2), nn.ReLU(), nn.Linear(h2, h1), nn.ReLU(),
                                 nn.Linear(h1, n_feat))

    def forward(self, x, c):
        z = self.enc(torch.cat([x, c], dim=1))
        return self.dec(torch.cat([z, c], dim=1))
