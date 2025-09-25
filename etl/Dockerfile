FROM python:3.10-slim

# Installer dépendances système
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    locales \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    && sed -i '/fr_FR.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen fr_FR.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=fr_FR.UTF-8
ENV LANGUAGE=fr_FR:fr
ENV LC_ALL=fr_FR.UTF-8

# Dépendances Python
RUN pip install --no-cache-dir pandas numpy requests python-dotenv selenium snowflake-connector-python[pandas]

# Créer dossier de travail et downloads
WORKDIR /app
RUN mkdir -p /app

# Chrome/Chromium binaries
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver