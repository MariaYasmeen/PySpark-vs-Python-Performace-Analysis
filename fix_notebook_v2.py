import json
import os

p = 'energy_project.ipynb'
nb = json.load(open(p, 'r', encoding='utf-8'))

# 1. Fix the helper functions cell (Cell 13 / In[14])
# We find it by looking for the definition of _save_fig or _barh
for c in nb['cells']:
    if c['cell_type'] == 'code':
        src = ''.join(c.get('source', []))
        if 'def _save_fig' in src and 'def _barh' in src:
            # Fix indentation for _save_fig calls and ensure rlike is safe
            new_lines = []
            for line in src.splitlines(True):
                # Fix the missing indent from previous attempt if it exists
                if line.strip() == '_save_fig(fig, filename)':
                    new_lines.append('    _save_fig(fig, filename)\n')
                else:
                    new_lines.append(line)
            c['source'] = new_lines

# 2. Update the final benchmarking cell (the very last cell)
# We use more appropriate row limits and a more robust comparison
final_bench_source = [
    "import time\n",
    "import csv\n",
    "import pandas as pd\n",
    "import matplotlib.pyplot as plt\n",
    "\n",
    "def benchmark_sql_vs_python_final(row_limits=[1000, 2000, 4000, 8000]):\n",
    "    results_final = []\n",
    "    # DATA_PATH should be available from earlier cells\n",
    "    path = DATA_PATH if \"DATA_PATH\" in globals() else \"pakistanenergyaccesshouseholdpanelsurveydata.csv\"\n",
    "    \n",
    "    # Warm-up Spark\n",
    "    print(\"Warming up Spark...\")\n",
    "    spark.read.option(\"header\", \"true\").csv(path).limit(100).collect()\n",
    "    \n",
    "    for n in row_limits:\n",
    "        print(f\"Benchmarking {n} rows...\")\n",
    "        \n",
    "        # 1. Spark SQL (including read time for fairness)\n",
    "        t0 = time.perf_counter()\n",
    "        df_tmp = spark.read.option(\"header\", \"true\").csv(path).limit(n)\n",
    "        df_tmp.createOrReplaceTempView(\"bench_final\")\n",
    "        # Using 'Region' as found in the CSV\n",
    "        spark.sql(\"SELECT Region, COUNT(*) as cnt FROM bench_final GROUP BY Region\").collect()\n",
    "        sql_time = time.perf_counter() - t0\n",
    "        \n",
    "        # 2. Pure Python (including open time)\n",
    "        t0 = time.perf_counter()\n",
    "        py_counts = {}\n",
    "        with open(path, 'r', encoding='utf-8-sig') as f:\n",
    "            reader = csv.DictReader(f)\n",
    "            count = 0\n",
    "            for row in reader:\n",
    "                reg = row.get('Region', 'UNKNOWN')\n",
    "                py_counts[reg] = py_counts.get(reg, 0) + 1\n",
    "                count += 1\n",
    "                if count >= n: break\n",
    "        python_time = time.perf_counter() - t0\n",
    "        \n",
    "        results_final.append({\n",
    "            \"rows\": n,\n",
    "            \"spark_sql_s\": sql_time,\n",
    "            \"pure_python_s\": python_time,\n",
    "            \"speedup\": python_time / sql_time if sql_time > 0 else 0\n",
    "        })\n",
    "    \n",
    "    return results_final\n",
    "\n",
    "final_results = benchmark_sql_vs_python_final()\n",
    "df_res = pd.DataFrame(final_results)\n",
    "\n",
    "fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))\n",
    "\n",
    "# Absolute Time Plot\n",
    "ax1.plot(df_res['rows'], df_res['pure_python_s'], marker='o', label='Pure Python', color='#ef4444', linewidth=2)\n",
    "ax1.plot(df_res['rows'], df_res['spark_sql_s'], marker='s', label='Spark SQL', color='#3b82f6', linewidth=2)\n",
    "ax1.set_title(\"Execution Time: Spark SQL vs Python (Lower is Better)\")\n",
    "ax1.set_xlabel(\"Number of Rows\")\n",
    "ax1.set_ylabel(\"Seconds\")\n",
    "ax1.legend()\n",
    "ax1.grid(True, alpha=0.3)\n",
    "\n",
    "# Speedup Plot\n",
    "ax2.bar(df_res['rows'].astype(str), df_res['speedup'], color='#8b5cf6', alpha=0.8)\n",
    "ax2.set_title(\"Speedup Factor (Python Time / Spark SQL Time)\")\n",
    "ax2.set_xlabel(\"Number of Rows\")\n",
    "ax2.set_ylabel(\"Speedup (x times faster)\")\n",
    "ax2.axhline(y=1.0, color='black', linestyle='--', alpha=0.5, label='Equal Performance')\n",
    "for i, v in enumerate(df_res['speedup']):\n",
    "    ax2.text(i, v + 0.05, f\"{v:.2f}x\", ha='center', fontweight='bold')\n",
    "ax2.grid(axis='y', alpha=0.3)\n",
    "\n",
    "plt.tight_layout()\n",
    "if \"_save_fig\" in globals():\n",
    "    _save_fig(fig, \"final_spark_sql_vs_python_correct\")\n",
    "plt.show()\n",
    "\n",
    "print(\"Final Benchmarking results:\")\n",
    "print(df_res.to_string(index=False))\n"
]

nb['cells'][-1]['source'] = final_bench_source

with open(p, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print("Successfully updated notebook with corrected logic and fixed indentation.")
