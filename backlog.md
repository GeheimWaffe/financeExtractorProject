# Fait
### Intégration automatique des fichiers
Ddans le processus : 
 - d'abord copie du fichier comptes dans le bureau vers un répertoire d'extract (staging)
 - puis exécution de l'import

En fait il  faudrait raisonner ainsi : 
1. Scan d'un répertoire et récolte de tous les fichiers se terminant en ods et ayant un masque
2. Copie de ces fichiers vers le staging
3. exécution de l'extraction
### Gestion multi-paramètres
A l'appel de l'application, je peux chaîner les paramètres pour exécuter conversion et chargement en une passe. 

# A Faire
### Calculer les lignes insérées
Lorsque je charge le dataframe, je veux récupérer le nombre de lignes chargées de la table. 