del /f /q VProEnterpriseServer.pyz 2>nul
python -m zipapp . -o VProEnterpriseServer.pyz -m "main:start_server"