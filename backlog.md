### Intégration automatique des fichiers
#### Problématique actuelle
dans le processus : 
 - d'abord copie du fichier comptes dans le bureau vers un répertoire d'extract (staging)
 - puis exécution de l'import

En fait il  faudrait raisonner ainsi : 
1. Scan d'un répertoire et récolte de tous les fichiers se terminant en ods et ayant un masque
2. Copie de ces fichiers vers le staging
3. exécution de l'extraction