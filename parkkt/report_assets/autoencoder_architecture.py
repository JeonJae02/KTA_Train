import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

# 실제 feature_cols에 직접 들어가는 원본 태그 5종만 표시
# (Pump_BuildUp은 모델 입력이 아니라 "가동 구간 필터", TK_Level_PV는 현재 모델에서 미사용)
RAW_VARS = [
    "목표 설정값 (g_s_SV_{PUMP_ID})",
    "아날로그 모터 출력값 (Ana_Out_{PUMP_ID})",
    "원료탱크 온도 (TK_Temp_PV_{TANK_ID})",
    "토출 압력 (Scale_Out___PT_{PUMP_ID})",
    "유량 (Scale_Out___FT_{PUMP_ID})",
]

fig, ax = plt.subplots(figsize=(18, 8.2))
ax.set_xlim(0, 18)
ax.set_ylim(0, 8.2)
ax.axis("off")
ax.set_title("Autoencoder 기반 펌프/모터/인버터 이상탐지 구조", fontsize=20, fontweight="bold", pad=16)

# 1) raw variable list box (5종)
raw_box = FancyBboxPatch((0.3, 3.7), 3.2, 3.3, boxstyle="round,pad=0.12,rounding_size=0.12",
                          fc="#eaf2ff", ec="#2b6cb0", lw=2.0)
ax.add_patch(raw_box)
ax.text(1.9, 6.55, "원본 입력 변수 (5종)", ha="center", va="center", fontsize=13.5, fontweight="bold")
for i, v in enumerate(RAW_VARS):
    ax.text(0.55, 5.9 - i * 0.62, "• " + v, ha="left", va="center", fontsize=11)

# 1-1) filter box (Pump_BuildUp) - 모델 입력이 아니라 데이터 선택 조건
filter_box = FancyBboxPatch((0.3, 1.5), 3.2, 1.6, boxstyle="round,pad=0.12,rounding_size=0.12",
                             fc="#fff9e6", ec="#b7791f", lw=1.8, linestyle="dashed")
ax.add_patch(filter_box)
ax.text(1.9, 2.3, "학습/추론 대상 구간 필터\nPump_BuildUp_{PUMP_ID} == 1\n(모델 입력값 아님)",
        ha="center", va="center", fontsize=10.5, linespacing=1.5, color="#7b341e")

# 2) preprocessing box
prep_box = FancyBboxPatch((4.15, 3.4), 2.35, 2.0, boxstyle="round,pad=0.12,rounding_size=0.12",
                           fc="#eafaf1", ec="#2f855a", lw=2.0)
ax.add_patch(prep_box)
ax.text(5.32, 4.4, "전처리 +\n파생피처 생성\n(14개 입력 피처)", ha="center", va="center", fontsize=11.5, linespacing=1.6)

# 3) Encoder funnel (14 -> 8 -> 4)
def layer_col(x, n, height=5.0, y0=1.6, color="#c05621"):
    ys = [y0 + height * (i + 0.5) / n for i in range(n)]
    for y in ys:
        ax.add_patch(plt.Circle((x, y), 0.1, fc=color, ec="none"))
    return ys

x_in, x_h, x_lat = 7.1, 8.5, 9.9
ys_in = layer_col(x_in, 14)
ys_h1 = layer_col(x_h, 8)
ys_lat = layer_col(x_lat, 4, color="#805ad5")

for y1 in ys_in:
    for y2 in ys_h1:
        ax.plot([x_in, x_h], [y1, y2], color="#cbd5e0", lw=0.4, zorder=0)
for y1 in ys_h1:
    for y2 in ys_lat:
        ax.plot([x_h, x_lat], [y1, y2], color="#cbd5e0", lw=0.4, zorder=0)

ax.text(x_in, 6.95, "입력층\n(14)", ha="center", fontsize=11.5)
ax.text(x_h, 6.95, "은닉층\n(8)", ha="center", fontsize=11.5)
ax.text(x_lat, 6.95, "잠재공간\n(4)", ha="center", fontsize=11.5, color="#553c9a", fontweight="bold")
ax.text((x_in + x_lat) / 2, 1.05, "Encoder", ha="center", fontsize=14, fontweight="bold", color="#c05621")

# 4) Decoder funnel (4 -> 8 -> 14)
x_h2, x_out = 11.3, 12.7
ys_h2 = layer_col(x_h2, 8)
ys_out = layer_col(x_out, 14)

for y1 in ys_lat:
    for y2 in ys_h2:
        ax.plot([x_lat, x_h2], [y1, y2], color="#cbd5e0", lw=0.4, zorder=0)
for y1 in ys_h2:
    for y2 in ys_out:
        ax.plot([x_h2, x_out], [y1, y2], color="#cbd5e0", lw=0.4, zorder=0)

ax.text(x_h2, 6.95, "은닉층\n(8)", ha="center", fontsize=11.5)
ax.text(x_out, 6.95, "출력층\n(14)", ha="center", fontsize=11.5)
ax.text((x_lat + x_out) / 2, 1.05, "Decoder", ha="center", fontsize=14, fontweight="bold", color="#2b6cb0")

# 5) reconstruction output box
recon_box = FancyBboxPatch((13.6, 3.4), 2.2, 2.0, boxstyle="round,pad=0.12,rounding_size=0.12",
                            fc="#fff5e6", ec="#c05621", lw=2.0)
ax.add_patch(recon_box)
ax.text(14.7, 4.4, "복원값\n(14개 피처)", ha="center", va="center", fontsize=11.5, linespacing=1.6)

# 6) reconstruction error comparison
err_box = FancyBboxPatch((16.15, 3.0), 1.7, 2.8, boxstyle="round,pad=0.12,rounding_size=0.12",
                          fc="#ffe8e8", ec="#c53030", lw=2.0)
ax.add_patch(err_box)
ax.text(17.0, 4.4, "재구성오차\n(MSE)\n\n> 임계값\n→ 이상 판정", ha="center", va="center", fontsize=11, linespacing=1.6)

arrow_style = dict(arrowstyle="-|>", mutation_scale=18, color="#4a5568", lw=1.8)
ax.add_patch(FancyArrowPatch((3.5, 4.4), (4.15, 4.4), **arrow_style))
ax.add_patch(FancyArrowPatch((3.5, 2.3), (4.4, 3.4), connectionstyle="arc3,rad=-0.2", **arrow_style))
ax.add_patch(FancyArrowPatch((6.5, 4.4), (7.0, 4.4), **arrow_style))
ax.add_patch(FancyArrowPatch((13.3, 4.4), (13.6, 4.4), **arrow_style))
ax.add_patch(FancyArrowPatch((15.8, 4.4), (16.15, 4.4), **arrow_style))

# original vs reconstructed comparison note (loop under)
ax.annotate(
    "", xy=(17.0, 3.0), xytext=(5.32, 3.4),
    arrowprops=dict(arrowstyle="-", color="#c53030", lw=1.5, linestyle="dashed",
                     connectionstyle="arc3,rad=-0.12")
)
ax.text(10.2, 0.3, "이상 시점: 원본값 vs 복원값의 변수별 오차 기여도(%)를 계산해 원인 변수(센서)까지 추정",
        ha="center", fontsize=13, color="#c53030")

plt.tight_layout()
out_path = "autoencoder_architecture.png"
plt.savefig(out_path, dpi=170, bbox_inches="tight")
print("saved:", out_path)
