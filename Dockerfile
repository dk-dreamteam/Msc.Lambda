FROM apache/spark:latest

# Γυρνάμε σε root χρήστη για να κάνουμε την εγκατάσταση
USER root

# Εγκατάσταση του pip και των απαραίτητων εργαλείων
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean

# Αντιγραφή και εγκατάσταση των python βιβλιοθηκών
COPY requirements.txt /opt/spark/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements.txt

# Επιστροφή στον κανονικό χρήστη spark για ασφάλεια
USER spark