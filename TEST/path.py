import os
print("ok")

# chemin courant
current_dir = os.path.dirname(os.path.abspath(__file__))

# dossier parent
parent_dir = os.path.dirname(current_dir)
parent_dir = parent_dir.replace("\\","/")

print("Dossier courant :", current_dir)
print("Dossier parent :", parent_dir)