"""Generate a 2-panel chart showing the two operationalized in-fighting metrics."""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Same data as gop_infighting_analysis.py
county_R = {2015: 1, 2016: 1, 2017: 1, 2018: 1, 2019: 1, 2020: 1, 2021: 23}
contested_R_primaries = {2018: 170, 2020: 224, 2022: 230, 2024: 189}
defeated_R_incumbents = {2014: 4, 2016: 5, 2018: 2, 2020: 5, 2022: 10, 2024: 2}

fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))

# --- Panel A: Censures ---
ax = axes[0]
years = sorted(county_R)
vals = [county_R[y] for y in years]
colors = ['#888' if y < 2021 else '#c0392b' for y in years]
ax.bar(years, vals, color=colors, edgecolor='black', linewidth=0.5)
ax.set_title("(a) County GOP censures of named Republican officials",
             fontsize=11, fontweight='bold')
ax.set_ylabel("Number of censures (per year)")
ax.set_xlabel("Year")
ax.axvline(2020.5, color='black', linestyle=':', linewidth=1, alpha=0.6)
ax.text(2020.55, 21, "2020 election\n→ Jan 6 →\nimpeachment vote",
        fontsize=8, va='top')
ax.text(2021, 23.5, "23", ha='center', fontsize=10, fontweight='bold')
ax.set_ylim(0, 27)
gray_patch = mpatches.Patch(color='#888', label='Pre-2021 baseline (~1/yr)')
red_patch = mpatches.Patch(color='#c0392b', label='2021 (23× baseline)')
ax.legend(handles=[gray_patch, red_patch], loc='upper left', fontsize=9)

# --- Panel B: Primary contests ---
ax = axes[1]
years_p = sorted(contested_R_primaries)
vals_p = [contested_R_primaries[y] for y in years_p]
colors_p = ['#888' if y < 2020 else '#c0392b' for y in years_p]
bars = ax.bar([y - 0.2 for y in years_p], vals_p, width=0.4,
              color=colors_p, edgecolor='black', linewidth=0.5,
              label='Contested R House primaries')

years_d = sorted(defeated_R_incumbents)
# Scale defeated count for dual visibility
ax2 = ax.twinx()
vals_d = [defeated_R_incumbents[y] for y in years_d]
colors_d = ['#333' if y < 2020 else '#7b241c' for y in years_d]
ax2.bar([y + 0.2 for y in years_d], vals_d, width=0.4,
        color=colors_d, alpha=0.7, edgecolor='black', linewidth=0.5,
        hatch='//', label='R House incumbents defeated')

ax.set_title("(b) Republican U.S. House primary competition",
             fontsize=11, fontweight='bold')
ax.set_ylabel("Contested Republican House primaries (bars, left axis)")
ax2.set_ylabel("Republican incumbents defeated in primary (hatched, right axis)")
ax.set_xlabel("Cycle")
ax.axvline(2020.5, color='black', linestyle=':', linewidth=1, alpha=0.6)
ax.set_xticks(sorted(set(years_p + years_d)))
ax.set_ylim(0, 260)
ax2.set_ylim(0, 12)

# Combined legend
lines_labels = [(bars, 'Contested R primaries'),
                (None, 'R incumbents defeated (hatched)')]
ax.legend(loc='upper left', fontsize=9)
ax2.legend(loc='upper right', fontsize=9)

fig.suptitle("Two empirical measures of intra-Republican conflict, 2014-2024",
             fontsize=13, fontweight='bold', y=1.02)
fig.text(0.5, -0.04,
         "Sources: Ballotpedia (Annual Congressional Competitiveness Reports, "
         "State Party Censures 2021); FiveThirtyEight (Skelley 2022).",
         ha='center', fontsize=8, style='italic')

plt.tight_layout()
plt.savefig('gop_infighting_chart.png', dpi=160, bbox_inches='tight')
print("saved gop_infighting_chart.png")
