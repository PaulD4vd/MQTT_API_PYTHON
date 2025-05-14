# Menggunakan image dasar Python 3.9
FROM python:3.10.12

# Set working directory di dalam container
WORKDIR /app

# Menyalin file requirements.txt ke dalam container
COPY requirements.txt .

# Install dependensi dari requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Menyalin seluruh kode aplikasi ke dalam container
COPY . .

# Membuka port MQTT dan MongoDB (atau port yang diperlukan aplikasi Anda)
EXPOSE 1883 2888

# Perintah untuk menjalankan aplikasi Python Anda
CMD ["python", "main.py"]
