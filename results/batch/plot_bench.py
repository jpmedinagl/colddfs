import pandas as pd
import matplotlib.pyplot as plt

# Data from CSV
data = pd.DataFrame({
    "type": ["prealloc", "postalloc", "batchalloc"],
    "create": [2.34, 0.01, 0.01],
    "write": [3.88, 6.12, 1.71],
    "read": [1.63, 1.60, 0.73],
    "total": [7.84, 7.72, 2.44]
})

# Set up the bar chart
fig, ax = plt.subplots(figsize=(10, 6))

# Positions for groups and bar width
x = range(len(data["type"]))
width = 0.2

# Plot each metric
ax.bar([i - 1.5*width for i in x], data["create"], width, label="create")
ax.bar([i - 0.5*width for i in x], data["write"], width, label="write")
ax.bar([i + 0.5*width for i in x], data["read"], width, label="read")
ax.bar([i + 1.5*width for i in x], data["total"], width, label="total")

# Labels and title
ax.set_xticks(list(x))
ax.set_xticklabels(data["type"])
ax.set_ylabel("Time (ms)")
ax.set_title("Latency per Operation (ms) vs Implementation")
ax.legend()
# ax.grid(True)
# plt.tight_layout()

# Show the plot
# plt.show()

plt.savefig("latency_impl.png")
