# 🚀 Step Installation

> Selamat datang! Ikuti langkah-langkah berikut untuk menjalankan project ini di lingkungan lokal Anda.

1. 📦 Clone repositories
```bash
git clone https://github.com/jordanistiqlal/recommendation-by-mal-history.git

cd recommendation-by-mal-history
```

2. 📚 Install requirements libraries
```bash
pip install -r requirements.txt
```

```
python3 -m venv .venv
source .venv/Scripts/activate # OS COMMAND
source .venv/bin/activate # CPANEL COMMAND

pip install -r requirements.txt
pip install --no-cache-dir -r requirements.txt
pip install -r requirements.txt --default-timeout=100 --retries=20
```

3. ▶️ Jalankan Program
```bash
python main.py

OR 

py main.py

OR

python -m flask run --no-reload --no-debugger
```

4. 📍After Update code set requirement.txt

```
pip freeze | grep -f requirements.txt > requirements-core.txt

```