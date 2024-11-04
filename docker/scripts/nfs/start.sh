#!/bin/sh
set -e

# Démarrer rpcbind
rpcbind

# Démarrer rpc.statd
rpc.statd

# Appliquer les configurations d'exportation
exportfs -r

# Démarrer les services NFS
rpc.nfsd
rpc.mountd -F

# Garder le conteneur en fonctionnement
exec /usr/sbin/rpc.nfsd
