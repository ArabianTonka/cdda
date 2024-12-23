import pandas as pd

# Charger les données avec le bon délimiteur (tabulation ou virgule selon le cas)
data = pd.read_csv("marketing_campaign.csv", delimiter="\t")  # Change delimiter if needed

# Afficher les noms des colonnes après le chargement
print("Colonnes après chargement :")
print(data.columns.tolist())

# Afficher les premières lignes pour voir les données
print("Premières lignes du dataset :")
print(data.head())
