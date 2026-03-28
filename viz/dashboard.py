# -*- coding: utf-8 -*-
import mysql.connector
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import pandas as pd
import os

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "spark",
    "password": "spark123",
    "database": "emploi_france"
}

output_dir = os.path.expanduser("~/emploi-france-lakehouse/viz/output")
os.makedirs(output_dir, exist_ok=True)

conn = mysql.connector.connect(**DB_CONFIG)

# --- GRAPHIQUE 1 : Top 10 métiers par nombre d'offres ---
df1 = pd.read_sql("""
    SELECT job_title, SUM(nb_offres) as total_offres
    FROM datamart_top_metiers
    GROUP BY job_title
    ORDER BY total_offres DESC
    LIMIT 10
""", conn)

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(df1['job_title'], df1['total_offres'], color='steelblue')
ax.set_xlabel('Nombre total d\'offres')
ax.set_title('Top 10 métiers qui recrutent le plus (tous pays confondus)')
ax.bar_label(bars, padding=3)
plt.tight_layout()
plt.savefig(f"{output_dir}/graph1_top_metiers.png", dpi=150)
plt.close()
print("Graph 1 OK")

# --- GRAPHIQUE 2 : Répartition des offres par type de contrat ---
df2 = pd.read_sql("""
    SELECT work_type, nb_combinaisons, avg_offres_par_metier
    FROM datamart_profils_demandes
    ORDER BY avg_offres_par_metier DESC
""", conn)

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

axes[0].pie(df2['nb_combinaisons'], labels=df2['work_type'],
            autopct='%1.1f%%', startangle=90,
            colors=['#2196F3','#4CAF50','#FF9800','#E91E63','#9C27B0'])
axes[0].set_title('Répartition des combinaisons par type de contrat')

axes[1].bar(df2['work_type'], df2['avg_offres_par_metier'], color='coral')
axes[1].set_xlabel('Type de contrat')
axes[1].set_ylabel('Moyenne d\'offres par métier')
axes[1].set_title('Moyenne d\'offres par métier selon le type de contrat')
axes[1].tick_params(axis='x', rotation=15)

plt.tight_layout()
plt.savefig(f"{output_dir}/graph2_contrats.png", dpi=150)
plt.close()
print("Graph 2 OK")

# --- GRAPHIQUE 3 : Top 15 pays par nombre d'offres ---
df3 = pd.read_sql("""
    SELECT country, SUM(nb_offres) as total_offres
    FROM datamart_top_metiers
    GROUP BY country
    ORDER BY total_offres DESC
    LIMIT 15
""", conn)

fig, ax = plt.subplots(figsize=(12, 7))
colors = plt.cm.viridis([i/len(df3) for i in range(len(df3))])
bars = ax.bar(df3['country'], df3['total_offres'], color=colors)
ax.set_xlabel('Pays')
ax.set_ylabel('Nombre total d\'offres')
ax.set_title('Top 15 pays par volume d\'offres d\'emploi')
ax.tick_params(axis='x', rotation=45)
ax.bar_label(bars, padding=3, fontsize=8)
plt.tight_layout()
plt.savefig(f"{output_dir}/graph3_pays.png", dpi=150)
plt.close()
print("Graph 3 OK")

conn.close()
print(f"\nVisualisations sauvegardées dans : {output_dir}")
