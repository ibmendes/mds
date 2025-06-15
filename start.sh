#!/bin/bash

set -e  # Encerra o script se qualquer comando falhar

# Navega até a pasta onde está o setup
cd src-docker

# Garante permissões e executa o setup
echo "[INFO] Executando setup.sh..."
chmod +x setup.sh
./setup.sh

# Volta para a raiz do projeto
cd ..

# Sobe os containers com build
echo "[INFO] Subindo containers com docker-compose..."
docker-compose up
