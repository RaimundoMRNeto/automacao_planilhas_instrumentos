# ETL SICONV – COPREC

Pipeline em Python para coletar e filtrar dados do SICONV (DETRU/MGI),
gerar Excel com:
- `convenios`
- `dicionario_variaveis`
- `info_execucao`

## Requisitos
```bash
python -m venv .venv
. .venv/Scripts/activate   # Windows
pip install -r requirements.txt
