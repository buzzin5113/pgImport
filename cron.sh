#/bin/bash
cd /opt/pgImport
docker build -t pgimport .
docker run -v /opt/pgImport:/app pgimport:latest