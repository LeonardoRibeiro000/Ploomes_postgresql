import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()
# URL base da API
url = "https://api2.ploomes.com/Fields"

# Sua User-Key
user_key = os.getenv("user_key")

# Cabeçalhos com a User-Key
headers = {
    'User-Key': user_key,  
    'Content-Type': 'application/json'
}



# Parâmetros para paginação
skip = 0
batch_size = 300  # A API retorna no máximo 300 registros por vez
all_deals = []

while True:
    # Fazendo a requisição GET com paginação
    response = requests.get(f"{url}?$skip={skip}", headers=headers)
    
    # Verificando se a solicitação foi bem-sucedida
    if response.status_code == 200:
        data = response.json()
        deals = data.get('value', [])

        if not deals:
            break  # Para o loop quando não houver mais registros

        all_deals.extend(deals)  # Adiciona os novos registros à lista
        skip += batch_size  # Atualiza o deslocamento para a próxima "página"
    
    elif response.status_code == 429:
        print("Erro 429: Muitas requisições. Aguarde antes de tentar novamente.")
        break
    
    else:
        print(f"Erro ao acessar a API: {response.status_code}")
        print(response.text)
        break

df = pd.json_normalize(all_deals)
print(df.head())
df.to_excel('fields.xlsx', index=False)



